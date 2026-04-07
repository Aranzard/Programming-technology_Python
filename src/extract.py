import requests
from requests.exceptions import Timeout, ConnectionError, HTTPError
import json
import yaml
from datetime import datetime, timedelta, UTC
from pathlib import Path

def fetch_json(url, params=None, timeout=10):
    """Запрос с проверкой статуса и разбором JSON"""
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        data = response.json()
        return {
            "ok": True,
            "status_code": response.status_code,
            "data": data,
            "url": response.url,
            "error_type": None,
            "error_message": None,
        }
    except Timeout as e:
        return {
            "ok": False,
            "status_code": None,
            "data": None,
            "url": url,
            "error_type": "Timeout",
            "error_message": f"Превышено время ожидания ({timeout} сек)",
        }
    except ConnectionError as e:
        return {
            "ok": False,
            "status_code": None,
            "data": None,
            "url": url,
            "error_type": "ConnectionError",
            "error_message": "Ошибка соединения с сервером",
        }
    except HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        return {
            "ok": False,
            "status_code": status,
            "data": None,
            "url": url,
            "error_type": "HTTPError",
            "error_message": f"HTTP ошибка {status}",
        }
    except json.JSONDecodeError as e:
        return {
            "ok": False,
            "status_code": response.status_code,
            "data": None,
            "url": response.url,
            "error_type": "JSONDecodeError",
            "error_message": "Ответ не является JSON",
        }

# Конфигурация
config_path = f"configs/variant_16.yml"

# Загрузка конфига
with open(config_path, 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

# Параметры запроса
params = config['api']['params'].copy()
end_date = datetime.now(UTC)
start_date = end_date - timedelta(days=30)
params['starttime'] = start_date.strftime('%Y-%m-%d')
params['endtime'] = end_date.strftime('%Y-%m-%d')

# Выполнение запроса
result = fetch_json(
    config['api']['base_url'],
    params=params,
    timeout=10
)

# Создание директории для сохранения
raw_dir = Path(f"../data/raw/variant_16/")
raw_dir.mkdir(parents=True, exist_ok=True)

# Формирование имени файла
timestamp = datetime.now(UTC).strftime('%Y-%m-%d_%H-%M-%S')
output_path = raw_dir / f"{timestamp}.json"

# Лог в консоль
print(f"Вариант: 16")
print(f"Источник: {config['source_type']}")
print(f"URL: {result['url']}")

if result["ok"]:
    data = result["data"]
    events = data.get('features', [])
    
    # Сохранение raw JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"Статус: {result['status_code']} OK")
    print(f"Сохранено: {output_path}")
    print(f"Получено данных: {len(events)} событий")
    
    if events:
        mags = [e['properties']['mag'] for e in events if e['properties'].get('mag')]
        if mags:
            print(f"Магнитуда: мин={min(mags):.1f}, макс={max(mags):.1f}")
else:
    print(f"Статус: {result['status_code'] if result['status_code'] else 'N/A'}")
    print(f"Ошибка: {result['error_type']} - {result['error_message']}")
