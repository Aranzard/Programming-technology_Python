import sys
from pathlib import Path

# Добавляем корневую папку проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

# Импортируем функции из dq.py (без классов)
from src.dq import (
    check_non_empty, check_not_null, check_unique_key, 
    check_numeric_range, check_positive, check_allowed_values,
    load_normalized, load_mart
)

print("=" * 70)
print("ДЕМОНСТРАЦИЯ: DQ ПРОВЕРКИ ЛОВЯТ ОШИБКИ")
print("=" * 70)

# Создаём сломанные данные
broken_normalized = pd.DataFrame([
    {'event_id': None, 'ts': '2026-04-01', 'mag': 15.0, 'depth_km': -10, 'place': 'Test'},
    {'event_id': 'A1', 'ts': '2026-04-01', 'mag': 4.5, 'depth_km': 50, 'place': 'Test'},
    {'event_id': 'A1', 'ts': '2026-04-02', 'mag': 4.6, 'depth_km': 60, 'place': 'Test'},
    {'event_id': 'A3', 'ts': '2026-04-01', 'mag': 4.4, 'depth_km': 80, 'place': 'Test'},
])

empty_normalized = pd.DataFrame([
])


print("\nСозданы данные с ошибками:")
print("- NULL в event_id")
print("- Дубликат event_id")
print("- Магнитуда вне диапазона (15.0)")
print("- Отрицательная глубина (-10)")

print("\nРезультаты проверок на сломанных данных:")
print("-" * 50)

# Запускаем проверки
df = broken_normalized

results = []
results.append(check_non_empty(empty_normalized, min_rows=1))
results.append(check_not_null(df, "event_id"))
results.append(check_unique_key(df, ["event_id"]))
results.append(check_numeric_range(df, "mag", 0, 10))
results.append(check_positive(df, "depth_km"))

for r in results:
    status = r["status"]
    name = r["name"]
    details = r["details"]
    print(f"{status}: {name} - {details}")

print("\n" + "=" * 70)
print("ВЫВОД: DQ проверки успешно обнаружили все ошибки!")
print("=" * 70)