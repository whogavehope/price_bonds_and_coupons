import requests
import time
import json
import pandas as pd
from datetime import datetime

# Список ISIN кодов облигаций
ISIN_LIST = [
    "SU26238RMFS4", "SU26233RMFS5", "SU26240RMFS0", "SU26218RMFS6",
    "SU26230RMFS1", "SU26225RMFS1", "RU000A106HB4", "RU000A104JQ3",
    "RU000A107MM9", "RU000A102T63", "RU000A103PX8", "SU26207RMFS9",
    "SU26219RMFS4", "SU26226RMFS9", "SU26229RMFS3", "SU26232RMFS7",
    "SU26243RMFS4", "SU29014RMFS6", "SU29016RMFS1", "SU29022RMFS9",
    "SU29025RMFS2", "RU000A106K43", "RU000A103D37", "RU000A106516"
]

# Базовый URL API Московской биржи (ISS MOEX)
BASE_URL = "https://iss.moex.com/iss"

def get_moex_bond_data(isin):
    """
    Получает название, дату погашения и график купонов для облигации по ISIN.
    Возвращает словарь с данными или None при ошибке.
    """
    security_details = {}
    coupons_list = []
    error_message = None

    # 1. Получение основной информации (название, дата погашения)
    security_info_url = f"{BASE_URL}/securities/{isin}.json"
    try:
        response_info = requests.get(security_info_url)
        response_info.raise_for_status()
        data_info = response_info.json()

        # Ищем данные в блоке 'description'
        if 'description' in data_info and 'data' in data_info['description']:
             desc_map = {item[0]: item[2] for item in data_info['description']['data']}
             security_details['name'] = desc_map.get('NAME', 'N/A')
             security_details['isin'] = isin # Добавляем ISIN для индекса

        # Ищем данные в блоке 'securities' (дублирование информации, но может быть надежнее)
        if 'securities' in data_info and 'data' in data_info['securities'] and data_info['securities']['data']:
            securities_cols = data_info['securities']['columns']
            securities_data = data_info['securities']['data'][0]
            data_map = dict(zip(securities_cols, securities_data))

            # Обновляем или добавляем данные
            if 'SECNAME' in data_map and security_details.get('name', 'N/A') == 'N/A':
                 security_details['name'] = data_map['SECNAME']
            security_details['maturity_date'] = data_map.get('MATDATE', 'N/A')
            security_details['face_value'] = data_map.get('FACEVALUE', 0)
            security_details['currency'] = data_map.get('CURRENCYID', 'N/A')


        if not security_details.get('name') or security_details.get('name') == 'N/A':
             error_message = "Не удалось получить название инструмента"


    except requests.exceptions.RequestException as e:
        error_message = f"Ошибка сети при получении инфо: {e}"
    except json.JSONDecodeError:
        error_message = "Ошибка декодирования JSON инфо"
    except Exception as e:
         error_message = f"Неизвестная ошибка при получении инфо: {e}"

    if error_message:
         print(f"Ошибка для {isin}: {error_message}")
         return {'isin': isin, 'error': error_message}

    # Добавляем небольшую паузу перед следующим запросом
    time.sleep(0.3)

    # 2. Получение графика купонов
    coupons_url = f"{BASE_URL}/securities/{isin}/coupons.json"
    try:
        response_coupons = requests.get(coupons_url)
        response_coupons.raise_for_status()
        data_coupons = response_coupons.json()

        if 'coupons' in data_coupons and 'data' in data_coupons['coupons'] and data_coupons['coupons']['data']:
             coupons_cols = data_coupons['coupons']['columns']
             coupon_data_list = data_coupons['coupons']['data']

             # Находим индексы нужных колонок
             try:
                 date_idx = coupons_cols.index('coupondate')
                 value_idx = coupons_cols.index('value') # Сумма купона в валюте номинала
             except ValueError:
                  error_message = "Не найдены колонки 'coupondate' или 'value' в данных купонов"
                  print(f"Ошибка для {isin}: {error_message}")
                  return {'isin': isin, 'error': error_message, **security_details} # Возвращаем что успели собрать

             for coupon_data in coupon_data_list:
                 coupon_date_str = coupon_data[date_idx]
                 coupon_value = coupon_data[value_idx]

                 # Обработка случая, когда сумма купона не указана (None)
                 if coupon_value is None:
                     coupon_value = 0.0

                 # Парсинг даты
                 try:
                     coupon_date = datetime.strptime(coupon_date_str, '%Y-%m-%d')
                     coupons_list.append({'date': coupon_date, 'amount': float(coupon_value)})
                 except (ValueError, TypeError):
                      print(f"Предупреждение для {isin}: Неверный формат даты '{coupon_date_str}' или суммы '{coupon_value}' купона, пропуск.")
                      continue # Пропускаем этот купон

        else:
             # Возможно, нет купонов или ошибка в ответе
             print(f"Информация для {isin}: Данные о купонах не найдены или пусты.")
             # Не считаем это критической ошибкой, просто купонов не будет

    except requests.exceptions.RequestException as e:
        # Если не удалось получить купоны, это не всегда критично, но сообщим
        print(f"Предупреждение для {isin}: Ошибка сети при получении купонов: {e}. Купоны не будут добавлены.")
        # error_message = f"Ошибка сети при получении купонов: {e}" # Можно сделать ошибку критичной
    except json.JSONDecodeError:
        print(f"Предупреждение для {isin}: Ошибка декодирования JSON купонов. Купоны не будут добавлены.")
        # error_message = "Ошибка декодирования JSON купонов"
    except Exception as e:
        print(f"Предупреждение для {isin}: Неизвестная ошибка при получении купонов: {e}. Купоны не будут добавлены.")
        # error_message = f"Неизвестная ошибка при получении купонов: {e}"

    # Возвращаем собранные данные
    return {
        **security_details, # name, maturity_date, face_value, currency, isin
        'coupons': coupons_list,
        'error': None if not error_message else error_message # Обновляем ошибку, если она возникла на этапе купонов
    }

# --- Основная часть скрипта ---

# Настройки pandas для вывода
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 50)
pd.set_option('display.width', 1000)

print("Получение данных облигаций с Московской биржи...")
print("-" * 60)

all_bond_data_list = []
processed_isins = []

for isin_code in ISIN_LIST:
    print(f"Обработка {isin_code}...")
    bond_data = get_moex_bond_data(isin_code)

    if bond_data and not bond_data.get('error'):
        all_bond_data_list.append(bond_data)
        processed_isins.append(isin_code)
    elif bond_data and bond_data.get('error'):
         print(f"-> Пропуск {isin_code} из-за ошибки: {bond_data.get('error')}")
         # Можно добавить заглушку в DataFrame, если нужно
         # processed_isins.append(isin_code) # Раскомментировать, если добавляем заглушку
    else:
         print(f"-> Пропуск {isin_code}, не получено данных.")


    time.sleep(0.7) # Задержка между запросами разных ISIN (0.3 сек внутри функции + 0.7 = 1 сек)

print("-" * 60)
print("Обработка данных и создание DataFrame...")

# Готовим структуру DataFrame
months_2025_ru = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
columns_2025 = [f'{m} 2025' for m in months_2025_ru]
final_columns = ['Название', 'Дата погашения'] + columns_2025

# Словарь для мэппинга номера месяца на имя колонки
month_map_2025 = {i+1: col_name for i, col_name in enumerate(columns_2025)}

dataframe_rows = []

for bond in all_bond_data_list:
    # Инициализируем купоны для 2025 года нулями
    monthly_coupons = {col_name: 0.0 for col_name in columns_2025}

    # Суммируем купоны по месяцам 2025 года
    if 'coupons' in bond:
        for coupon in bond['coupons']:
            coupon_date = coupon['date']
            # Фильтруем по 2025 году
            if coupon_date.year == 2025:
                month_num = coupon_date.month
                col_name = month_map_2025.get(month_num)
                if col_name:
                    # Добавляем сумму купона к соответствующему месяцу
                    monthly_coupons[col_name] += coupon.get('amount', 0.0) # Используем get для безопасности

    # Создаем строку для DataFrame
    row_data = {
        'Название': bond.get('name', 'N/A'),
        'Дата погашения': bond.get('maturity_date', 'N/A'),
        **monthly_coupons # Добавляем рассчитанные купоны по месяцам
    }
    dataframe_rows.append(row_data)

# Создаем DataFrame
if dataframe_rows:
    df = pd.DataFrame(dataframe_rows, index=processed_isins)
    # Убедимся, что колонки идут в нужном порядке
    df = df[final_columns]
else:
    # Если данных нет, создаем пустой DataFrame с нужными колонками
    df = pd.DataFrame(columns=final_columns)
    df.index.name = 'ISIN'


print("-" * 60)
print("Итоговый DataFrame с купонными выплатами за 2025 год:")
print(df)
print("-" * 60)
