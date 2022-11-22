# Мини библиотека полезных функицй

import requests
import csv
import datetime
import time
import random
import json

import vk_api
import gspread



def multiline_input():

    contents = []
    print('Вставьте текст. После перенесите строку и нажмите Ctrl+D.')
    while True:
        try:
            line = input()
        except EOFError:
            break
        contents.append(line)

    text = ''
    for line in contents:
        text += (line+'\n')
    return text


def send_stat(text):  # отправка сообщения в телеграм чат Sc Stats
	URL = 'https://api.telegram.org/bot5653226494' \
		  ':AAHe16DCW5Eziw4_tLI5eN-RJQJNUbrS2Oc/'
	CHAT_ID = '-1001503897344'   # SC Stats Telegram Group
	while True:
		try:
			result = requests.get(
				URL + 'sendMessage',
				params={
					'chat_id': CHAT_ID,
					'text': text
				}
			)
		except requests.exceptions as e:
			print(f'{datetime.datetime.now()}: {e}')
			time.sleep(30)
		break


def send_personal(text):  # отправка пресонального сообщения телеграм
	URL = 'https://api.telegram.org/bot5653226494' \
		  ':AAHe16DCW5Eziw4_tLI5eN-RJQJNUbrS2Oc/'
	CHAT_ID = '5309563931'   # Булат Богданов
	while True:
		try:
			result = requests.get(
				URL + 'sendMessage',
				params={
					'chat_id': CHAT_ID,
					'text': text
				}
			)
		except requests.exceptions as e:
			print(f'{datetime.datetime.now()}: {e}')
			time.sleep(30)
		break



# Через эту функцию делаю все вызовы к Google Sheets,
# т.к. она перехватывает ошибки и повторяет запросы в их случае
def gspread_function(func, args=None, returns_none=False):
	# print(f'gspread_function, func {func}, args {args}')    # for debug
	FATAL_ERRORS = [400, 401]

	result = None
	wait = 1
	start_time = datetime.datetime.now()
	while True:
		try:
			if type(args) is tuple:
				result = func(*args)
			elif args is None:
				result = func()
			else:
				result = func(args)

		except gspread.exceptions.APIError as e:
			e = str(e)
			error_code = ''
			for symbol in e[e.find('code'):]:
				if symbol.isdigit():
					error_code += symbol
				elif symbol == ',':
					error_code = int(error_code)
					break

			if error_code in FATAL_ERRORS:
				print(f'func: {func}')
				print(f'args: {args}')
				raise

			print(f'{error_code}, {func}, {args}')
			if datetime.datetime.now() - start_time > datetime.timedelta(minutes=30):
				print('sleep 20 min..')
				time.sleep(1200)
				start_time = datetime.datetime.now()
				wait = 1

			time.sleep(2 ** wait)
			time.sleep(random.random())
			if wait < 7:
				wait += 1
		else:
			if returns_none == False:
				if result is None:
					continue
			break
	return result


def normalize_phone(phone):
    phone = str(phone)
    phone_onlydigits = ''
    for symbol in phone:
        if symbol.isdigit():
            phone_onlydigits += symbol
    return phone_onlydigits[1:]