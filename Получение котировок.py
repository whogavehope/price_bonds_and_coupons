import requests
import json
import pandas as pd

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
    isin_query = ",".join(TARGET_ISIN_SET)
    url = f"{BASE_URL}/engines/stock/markets/bonds/securities.json?securities={isin_query}&iss.meta=off"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if 'securities' in data and 'columns' in data['securities'] and 'data' in data['securities']:
            cols = data['securities']['columns']
            rows = data['securities']['data']
            
            # Находим индексы нужных колонок
            try:
                isin_idx = cols.index('SECID')  # ISIN часто в SECID
                name_idx = cols.index('SECNAME')
                couponvalue_idx = cols.index('COUPONVALUE')  # Размер купона
                
                # >>> ДОБАВЬТЕ НОВЫЕ ПОЛЯ ЗДЕСЬ <<<
                # Например, если хотите добавить поле "MATDATE" (дата погашения):
                # matdate_idx = cols.index('MATDATE')
                
            except ValueError as e:
                print(f"Ошибка: Не найдена необходимая колонка ({e}).")
                return {}  # Возвращаем пустой словарь при ошибке структуры
            
            for row in rows:
                isin = row[isin_idx]
                if isin in TARGET_ISIN_SET:
                    securities_data[isin] = {
                        'isin': isin,
                        'name': row[name_idx],
                        'coupon_value': float(row[couponvalue_idx]) if row[couponvalue_idx] else 0.0,
                        
                        # >>> ДОБАВЬТЕ НОВЫЕ ПОЛЯ ЗДЕСЬ <<<
                        # Например, если добавили "MATDATE":
                        # 'maturity_date': row[matdate_idx],
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

# --- Основная часть скрипта ---
print("Этап 1: Загрузка информации о целевых облигациях...")
all_securities_info = fetch_target_securities()
print(f"-> Найдено {len(all_securities_info)} ISIN из целевого списка.")

print("-" * 60)
print("Этап 2: Создание DataFrame...")

# Готовим структуру DataFrame
dataframe_rows = []
for isin, details in all_securities_info.items():
    row_data = {
        'Название': details.get('name', 'N/A'),
        'Код (ISIN)': details.get('isin', 'N/A'),
        'Размер купона': details.get('coupon_value', 0.0),
        
        # >>> ДОБАВЬТЕ НОВЫЕ ПОЛЯ ЗДЕСЬ <<<
        # Например, если добавили "MATDATE":
        # 'Дата погашения': details.get('maturity_date', 'N/A'),
    }
    dataframe_rows.append(row_data)

# Создаем DataFrame
if dataframe_rows:
    df = pd.DataFrame(dataframe_rows)
else:
    df = pd.DataFrame(columns=['Название', 'Код (ISIN)', 'Размер купона'])

print("-" * 60)
print("Итоговый DataFrame:")
print(df)
print("-" * 60)
print("Завершено.")
