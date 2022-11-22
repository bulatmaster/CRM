# старая версия main.py , не используется

import random
import datetime
import time
import json

import gspread
from dadata import Dadata

from features import send_stat, gspread_function, send_personal



SOURCE_SHEET = ' АНКЕТА И ЗАЯВКИ 2'
DESTINATION_SHEET = 'Заявки SC'
PHONE_COLUMN = 2
TIMEZONE_INFO_COLUMN = 3
service_account = gspread.service_account(filename='data/service_account.json')
last_readed_row_file = 'data/sales_last_readed_row.txt'


def get_last_timestamp():
	sh = gspread_function(service_account.open, DESTINATION_SHEET)
	wks = gspread_function(sh.get_worksheet, 0)
	column = gspread_function(wks.col_values, 1)
	column.reverse()

	n = -1
	for value in column:
		try:
			datetime.datetime.strptime(value, '%d.%m.%Y %H:%M:%S')
			return value
		except ValueError:
			continue


def get_new_rows(last_timestamp):
	rows = []
	sh = gspread_function(service_account.open, SOURCE_SHEET)
	wks = gspread_function(sh.get_worksheet, 0)

	last_copied_row = gspread_function(wks.find, last_timestamp).row
	last_filled_row = len(gspread_function(wks.col_values, 1))
	if last_copied_row != last_filled_row:
		rows = gspread_function(wks.get_values, f'{last_copied_row + 1}:{last_filled_row}')
	return rows


def update_last_timestamp(rows):
	return rows[-1][0]


def filter_rows(rows):
	new_list = []
	for row in rows:
		if not row:
			continue
		try:
			phone = row[PHONE_COLUMN]
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


def write_rows(rows):

	sh = gspread_function(service_account.open, DESTINATION_SHEET)
	wks = gspread_function(sh.get_worksheet, 0)
	gspread_function(wks.append_rows, (rows, 'USER_ENTERED'))


def add_timezone_info(rows):

	with open('data/dadata.json') as f:
		data = json.loads(f.read())
		dadata = Dadata(data['token'], data['secret'])

	for row in rows:
		result = dadata.clean('phone', row[PHONE_COLUMN])
		info = f"{result['country']}, {result['region']}, {result['city']}, {result['timezone']}"
		info = info.replace(', None', '')
		if info == 'None':
			info = 'Номер не распознан'
		row[TIMEZONE_INFO_COLUMN] = info

	return rows


def main():
	try:
		last_timestamp = ''
		while True:
			if not last_timestamp:
				last_timestamp = get_last_timestamp()
			answers = get_new_rows(last_timestamp)
			if answers:
				last_timestamp = update_last_timestamp(answers)
				answers = filter_rows(answers)
				answers = add_timezone_info(answers)
				write_rows(answers)

			time.sleep(450)
	except Exception as e:
		send_stat(f'🔴sales.py crashes')
		send_personal('🔴sales.py crashes')
		raise


if __name__ == '__main__':
	main()