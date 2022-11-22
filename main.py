# Основной файл программы, должен крутиться на сервере.
# Программа в бесконечном цикле берет строки из таблиц, привязанных к гугл формам, где собираются заявки,
# оставляет только строки с валидными номерами телефонов, дополняет предыдущей инфой о книентах, информацией о часовом
# поясе, и складывает в другую таблицу.
#
# В файле data/sheepairs настраивается - откуда копировать, куда копировать
# В файле data/sheetstocompare - таблицы, откуда брать предыдущую инфу о клиенте
# см. документацию gspread
# Для нормальной работы первые столбцы должны быть всегда эти (BASE COLUMN INDEXES в коде):
# 1 столбец - Отметка времени, 2 - Имя клиента, 3 - телефон клиента, 4 - инфо о номере, 5 - инфо о клиенте
# для инфо о номере используется сервис Dadata
# Servive аккаунт заведите свой, я отключу который есть сейчас, тк он привязан к моему личному гуглу



import time
import json
import csv
import re
import datetime
import httpx

import gspread
from dadata import Dadata

from features import send_stat, gspread_function, send_personal, normalize_phone

pause_between_checks_secs = 60
sheet_pairs_file = 'data/sheetpairs.csv'
sheets_to_compare_file = 'data/sheetstocompare.csv'
google_service_account_file = 'data/service_account.json'
dadata_tokens_file = 'data/dadata_tokens.json'
MANAGERS_NAME_COLUMN_NAME = 'Кто звонил'

# BASE COLUMN INDEXES:   # Все таблицы должны быть такими
TIMESTAMP_COL = 0
CLIENT_NAME_COL = 1
PHONE_COL = 2
PHONE_INFO_COL = 3
MANAGERS_NAME_AND_INFO_COL = 4
MANAGERS_NAME_AND_INFO_COL_LETTER = 'E'


def get_updates(sheet_pair):
    sa = gspread.service_account(google_service_account_file)
    sh = gspread_function(sa.open_by_url, sheet_pair['Source Spreadsheet URL'])
    wks = gspread_function(sh.get_worksheet_by_id, int(sheet_pair['Source Worksheet ID']))
    last_timestamp = get_last_timestamp(sheet_pair)
    data = gspread_function(wks.get_values)

    last_copied_row = None
    for n, line in enumerate(data):
        if last_timestamp == line[0]:
            last_copied_row = n
    if last_copied_row is None:
        send_personal('CRM/main.py: WARNING not found last copied row')
        print('WARNING not found last copied row')
        send_personal('WARNING: CRM not found last copied row')
        return []

    last_filled_row = len(data)

    new_clients = []
    for row in data[last_copied_row+1:last_filled_row]:
        new_clients.append({'sheet_row': row})
    return new_clients


def get_last_timestamp(sheet_pair):
    sa = gspread.service_account(google_service_account_file)
    sh = gspread_function(sa.open_by_url, sheet_pair['Destination Spreadsheet URL'])
    wks = gspread_function(sh.get_worksheet_by_id, int(sheet_pair['Destination Worksheet ID']))
    column0_values = gspread_function(wks.col_values, 1)
    column0_values.reverse()
    return column0_values[0]


def remove_with_no_phones(rows):
    new_list = []
    for row in rows:
        if not row['sheet_row']:
            continue
        try:
            phone = row['sheet_row'][PHONE_COL]
        except IndexError:
            continue
        if not phone:
            continue
        if len(phone) < 6:
            continue
        digits_count = 0
        for symbol in phone:
            if symbol.isdigit():
                digits_count += 1
        if digits_count < 7:
            continue
        new_list.append(row)
    return new_list


def get_client_info(clients):
    worksheet_data_list = []
    sa = gspread.service_account(google_service_account_file)
    with open(sheets_to_compare_file) as f:
        dictreader = csv.DictReader(f)
        for row in dictreader:
            sh = gspread_function(sa.open_by_url, row['Spreadsheet URL'])
            for wks in gspread_function(sh.worksheets):
                worksheet_data_list.append(gspread_function(wks.get_values))

    for client in clients:
        phone = client['sheet_row'][PHONE_COL]
        if not phone:
            continue
        managers_name = ''
        client_info = ''

        for worksheet_data in worksheet_data_list:
            try:
                headers = worksheet_data[0]
            except IndexError:
                continue

            managers_name_col = None
            for n, header in enumerate(headers):
                if MANAGERS_NAME_COLUMN_NAME in header:
                    managers_name_col = n
                    break

            for row in worksheet_data[1:]:
                if row == client['sheet_row']:
                    continue
                for cell in row:
                    if not normalize_phone(phone) in normalize_phone(cell):
                        continue
                    if managers_name_col:
                        if row[managers_name_col]:
                            if managers_name:
                                managers_name += '\n'
                            managers_name += row[managers_name_col].strip()
                    for i, cell_2nd_cycle in enumerate(row):
                        if i == managers_name_col:
                            continue
                        if not cell_2nd_cycle:
                            continue
                        if client_info:
                            client_info += '\n'
                        if headers[i]:
                            client_info += headers[i] + ': '
                        client_info += cell_2nd_cycle
        if managers_name:
            managers_name = '\n'.join(set(managers_name.splitlines()))   # Удалить дубликаты
            client['sheet_row'][MANAGERS_NAME_AND_INFO_COL] = managers_name
        if client_info:
            if not managers_name:
                client['sheet_row'][MANAGERS_NAME_AND_INFO_COL] = '👀'
            client['info'] = client_info
    return clients



def google_write(clients, sheet_pair):
    sa = gspread.service_account(google_service_account_file)
    sh = gspread_function(sa.open_by_url, sheet_pair['Destination Spreadsheet URL'])
    wks = gspread_function(sh.get_worksheet_by_id, int(sheet_pair['Destination Worksheet ID']))
    rows = []
    for client in clients:
        rows.append(client['sheet_row'])
    gspread_function(wks.add_rows, len(rows), returns_none=True)
    first_empty_row = len(gspread_function(wks.col_values, 1)) + 1
    result = gspread_function(wks.update, (f'A{first_empty_row}', rows))

    try:
        up, down = result['updatedRange'].split('!')[1].split(':')
    except ValueError:
        return
    up = int(re.sub('[^0-9]', '', up))
    down = int(re.sub('[^0-9]', '', down))

    for client_index, row_index in enumerate(range(up, down+1)):
        if not 'info' in clients[client_index]:
            continue
        gspread_function(
            wks.insert_note,
            (f'{MANAGERS_NAME_AND_INFO_COL_LETTER}{row_index}', clients[client_index]['info']),
            returns_none=True
        )


def get_phone_info(clients):
    with open(dadata_tokens_file) as f:
        creds = json.loads(f.read())
    dadata = Dadata(creds['dadata_token'], creds['dadata_secret'])

    for client in clients:
        try:
            result = dadata.clean('phone', client['sheet_row'][PHONE_COL])
        except httpx.HTTPStatusError:
            continue
        info = f"{result['country']}, {result['region']}, {result['city']}, {result['timezone']}"
        info = info.replace(', None', '')
        if info == 'None':
            info = 'Номер не распознан'
        else:
            client['sheet_row'][PHONE_COL] = result['phone']
        client['sheet_row'][PHONE_INFO_COL] = info
    return clients


def main():
    try:
        while True:
            with open(sheet_pairs_file) as f:
                dictreader = csv.DictReader(f)
                for sheet_pair in dictreader:
                    clients = get_updates(sheet_pair)   # clients is Dict, 'sheet_row': [row], 'info': client_info
                    clients = remove_with_no_phones(clients)
                    if not clients:
                        continue
                    clients = get_client_info(clients)
                    clients = get_phone_info(clients)
                    google_write(clients, sheet_pair)
            time.sleep(pause_between_checks_secs)
    except Exception as e:
        send_stat(f'🔴 Выключился перенос номеров ')
        send_personal('🔴CRM main.py crash: ' + str(e))
        raise


if __name__ == '__main__':
    main()
