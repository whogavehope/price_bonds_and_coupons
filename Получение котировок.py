import requests
import time
import json
import pandas as pd
from datetime import datetime

# Список ISIN кодов облигаций, которые нас интересуют
TARGET_ISIN_LIST = [
    "SU26238RMFS4", "SU26233RMFS5", "SU26240RMFS0", "SU26218RMFS6",
    "SU26230RMFS1", "SU26225RMFS1", "RU000A106HB4", "RU000A104JQ3",
    "RU000A107MM9", "RU000A102T63", "RU000A103PX8", "SU26207RMFS9",
    "SU26219RMFS4", "SU26226RMFS9", "SU26229RMFS3", "SU26232RMFS7",
    "SU26243RMFS4", "SU29014RMFS6", "SU29016RMFS1", "SU29022RMFS9",
    "SU29025RMFS2", "RU000A106K43", "RU000A103D37", "RU000A106516"
]

# Преобразуем в set для быстрого поиска
TARGET_ISIN_SET = set(TARGET_ISIN_LIST)

# Базовый URL API Московской биржи (ISS MOEX)
BASE_URL = "https://iss.moex.com/iss"

def fetch_target_securities():
    """
    Загружает информацию о бумагах из TARGET_ISIN_SET.
    Возвращает словарь {isin: {details...}}
    """
    securities_data = {}
    # Добавляем все ISIN в параметр securities
    isin_query = ",".join(TARGET_ISIN_SET)
    url = f"{BASE_URL}/engines/stock/markets/bonds/securities.json?securities={isin_query}&iss.meta=off"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # Извлечение данных из блока 'securities'
        if 'securities' in data and 'columns' in data['securities'] and 'data' in data['securities']:
            cols = data['securities']['columns']
            rows = data['securities']['data']
            
            # Находим индексы нужных колонок один раз
            try:
                isin_idx = cols.index('SECID')  # ISIN часто в SECID
                name_idx = cols.index('SECNAME')
                matdate_idx = cols.index('MATDATE')
                faceval_idx = cols.index('FACEVALUE')
                currency_idx = cols.index('CURRENCYID')
            except ValueError as e:
                print(f"Ошибка: Не найдена необходимая колонка ({e}).")
                return {}  # Возвращаем пустой словарь при ошибке структуры
            
            # Обрабатываем строки
            for row in rows:
                isin = row[isin_idx]
                if isin in TARGET_ISIN_SET:
                    securities_data[isin] = {
                        'isin': isin,
                        'name': row[name_idx],
                        'maturity_date': row[matdate_idx],
                        'face_value': row[faceval_idx],
                        'currency': row[currency_idx],
                    }
        else:
            print("Блок 'securities' не найден в ответе или пуст.")
    except requests.exceptions.RequestException as e:
        print(f"Ошибка сети при загрузке данных: {e}")
    except json.JSONDecodeError:
        print("Ошибка декодирования JSON.")
    except Exception as e:
        print(f"Неизвестная ошибка: {e}")
    
    return securities_data

def get_coupon_data(isin):
    """
    Получает и парсит данные о купонах для одного ISIN.
    Возвращает список купонов или пустой список при ошибке.
    """
    coupons_list = []
    coupons_url = f"{BASE_URL}/securities/{isin}/coupons.json?iss.meta=off"
    try:
        response_coupons = requests.get(coupons_url)
        response_coupons.raise_for_status()
        data_coupons = response_coupons.json()
        
        if 'coupons' in data_coupons and 'columns' in data_coupons['coupons'] and 'data' in data_coupons['coupons']:
            coupons_cols = data_coupons['coupons']['columns']
            coupon_data_list = data_coupons['coupons']['data']
            
            try:
                date_idx = coupons_cols.index('coupondate')
                value_idx = coupons_cols.index('value')
            except ValueError:
                print(f"Предупреждение для {isin}: Не найдены колонки 'coupondate' или 'value' в данных купонов.")
                return []  # Возвращаем пустой список
            
            for coupon_data in coupon_data_list:
                coupon_date_str = coupon_data[date_idx]
                coupon_value = coupon_data[value_idx]
                if coupon_value is None:
                    coupon_value = 0.0
                try:
                    coupon_date = datetime.strptime(coupon_date_str, '%Y-%m-%d')
                    coupons_list.append({'date': coupon_date, 'amount': float(coupon_value)})
                except (ValueError, TypeError):
                    print(f"Предупреждение для {isin}: Неверный формат даты '{coupon_date_str}' или суммы '{coupon_value}' купона, пропуск.")
                    continue
    except requests.exceptions.RequestException as e:
        print(f"Предупреждение для {isin}: Ошибка сети при получении купонов: {e}.")
    except json.JSONDecodeError:
        print(f"Предупреждение для {isin}: Ошибка декодирования JSON купонов.")
    except Exception as e:
        print(f"Предупреждение для {isin}: Неизвестная ошибка при получении купонов: {e}.")
    
    return coupons_list

# --- Основная часть скрипта ---
print("Этап 1: Загрузка информации о целевых облигациях...")
all_securities_info = fetch_target_securities()
print(f"-> Найдено {len(all_securities_info)} ISIN из целевого списка.")

print("-" * 60)
print("Этап 2: Загрузка данных о купонах для найденных облигаций...")
processed_data_for_df = []
processed_isins_for_index = []

for isin, details in all_securities_info.items():
    print(f"  Получение купонов для {isin} ({details.get('name', '')[:30]}...).")
    coupons = get_coupon_data(isin)
    details['coupons'] = coupons  # Добавляем список купонов к данным облигации
    processed_data_for_df.append(details)
    processed_isins_for_index.append(isin)
    time.sleep(0.6)  # Задержка между запросами купонов разных ISIN

print("-" * 60)
print("Этап 3: Обработка данных и создание DataFrame...")
# Готовим структуру DataFrame
months_2025_ru = ['Янв', 'Фев', 'Мар', 'Апр', 'Май', 'Июн', 'Июл', 'Авг', 'Сен', 'Окт', 'Ноя', 'Дек']
columns_2025 = [f'{m} 2025' for m in months_2025_ru]
final_columns = ['Название', 'Дата погашения'] + columns_2025
month_map_2025 = {i+1: col_name for i, col_name in enumerate(columns_2025)}

dataframe_rows = []
for bond in processed_data_for_df:
    monthly_coupons = {col_name: 0.0 for col_name in columns_2025}
    if 'coupons' in bond:
        for coupon in bond['coupons']:
            coupon_date = coupon['date']
            if coupon_date.year == 2025:
                month_num = coupon_date.month
                col_name = month_map_2025.get(month_num)
                if col_name:
                    monthly_coupons[col_name] += coupon.get('amount', 0.0)
    row_data = {
        'Название': bond.get('name', 'N/A'),
        'Дата погашения': bond.get('maturity_date', 'N/A'),
        **monthly_coupons
    }
    dataframe_rows.append(row_data)

# Создаем DataFrame
if dataframe_rows:
    df = pd.DataFrame(dataframe_rows, index=processed_isins_for_index)
    df = df[final_columns]  # Упорядочиваем колонки
    df.index.name = 'ISIN'  # Называем индекс
else:
    df = pd.DataFrame(columns=final_columns)
    df.index.name = 'ISIN'

print("-" * 60)
print("Итоговый DataFrame с купонными выплатами за 2025 год:")
print(df)
print("-" * 60)
print("Завершено.")
