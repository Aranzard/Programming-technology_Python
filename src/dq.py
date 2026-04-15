import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional


def get_latest_file(data_dir: Path, subdir: str, pattern: str) -> Path:
    """Находит последний файл по шаблону"""
    dir_path = data_dir / subdir / "variant_16"
    files = list(dir_path.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {dir_path} with pattern {pattern}")
    return max(files, key=lambda f: f.stat().st_mtime)


# ========== БАЗОВЫЕ ПРОВЕРКИ ==========

def check_non_empty(df: pd.DataFrame, min_rows: int = 1, level: str = "FAIL") -> dict:
    """Проверка: таблица не пустая"""
    result = {
        "name": "non_empty",
        "level": level,
        "description": f"Таблица должна содержать минимум {min_rows} записей",
        "status": "PASS",
        "details": "",
        "rows_affected": len(df)
    }
    
    if len(df) < min_rows:
        result["status"] = "FAIL"
        result["details"] = f"Таблица содержит {len(df)} записей, ожидалось минимум {min_rows}"
    else:
        result["details"] = f"Таблица содержит {len(df)} записей"
    
    return result


def check_not_null(df: pd.DataFrame, column: str, level: str = "FAIL") -> dict:
    """Проверка: колонка не содержит NULL"""
    null_count = df[column].isnull().sum()
    
    result = {
        "name": f"not_null_{column}",
        "level": level,
        "description": f"Колонка '{column}' не должна содержать NULL",
        "status": "PASS",
        "details": "",
        "rows_affected": null_count
    }
    
    if null_count > 0:
        result["status"] = level
        result["details"] = f"Найдено {null_count} NULL значений в колонке '{column}'"
    else:
        result["details"] = f"Нет NULL значений в колонке '{column}'"
    
    return result


def check_unique_key(df: pd.DataFrame, key_columns: List[str], level: str = "FAIL") -> dict:
    """Проверка: уникальность бизнес-ключа"""
    duplicates = df.groupby(key_columns).size().reset_index(name='count')
    duplicates = duplicates[duplicates['count'] > 1]
    
    result = {
        "name": f"unique_key_{'_'.join(key_columns)}",
        "level": level,
        "description": f"Комбинация {key_columns} должна быть уникальной",
        "status": "PASS",
        "details": "",
        "rows_affected": len(duplicates)
    }
    
    if len(duplicates) > 0:
        result["status"] = level
        sample = duplicates.head(3).to_dict('records')
        result["details"] = f"Найдено {len(duplicates)} дублирующихся комбинаций. Примеры: {sample}"
    else:
        result["details"] = "Все ключи уникальны"
    
    return result


def check_numeric_range(df: pd.DataFrame, column: str, min_value: float, max_value: float, 
                         level: str = "WARNING") -> dict:
    """Проверка: числовые значения в диапазоне"""
    invalid = df[(df[column] < min_value) | (df[column] > max_value)]
    
    result = {
        "name": f"range_{column}",
        "level": level,
        "description": f"Колонка '{column}' должна быть в диапазоне [{min_value}, {max_value}]",
        "status": "PASS",
        "details": "",
        "rows_affected": len(invalid)
    }
    
    if len(invalid) > 0:
        result["status"] = level
        result["details"] = f"Найдено {len(invalid)} значений вне диапазона"
    else:
        result["details"] = f"Все значения в диапазоне: min={df[column].min():.1f}, max={df[column].max():.1f}"
    
    return result


def check_positive(df: pd.DataFrame, column: str, level: str = "WARNING") -> dict:
    """Проверка: значения положительные (> 0)"""
    invalid = df[df[column] <= 0]
    
    result = {
        "name": f"positive_{column}",
        "level": level,
        "description": f"Колонка '{column}' должна содержать только положительные значения (>0)",
        "status": "PASS",
        "details": "",
        "rows_affected": len(invalid)
    }
    
    if len(invalid) > 0:
        result["status"] = level
        result["details"] = f"Найдено {len(invalid)} неположительных значений"
    else:
        result["details"] = f"Все значения положительные: min={df[column].min()}"
    
    return result


def check_allowed_values(df: pd.DataFrame, column: str, allowed_values: List, level: str = "FAIL") -> dict:
    """Проверка: значения из допустимого списка"""
    invalid = df[~df[column].isin(allowed_values)]
    
    result = {
        "name": f"allowed_values_{column}",
        "level": level,
        "description": f"Колонка '{column}' должна содержать только допустимые значения: {allowed_values}",
        "status": "PASS",
        "details": "",
        "rows_affected": len(invalid)
    }
    
    if len(invalid) > 0:
        result["status"] = level
        unique_invalid = invalid[column].unique().tolist()
        result["details"] = f"Найдены недопустимые значения: {unique_invalid}"
    else:
        result["details"] = "Все значения допустимы"
    
    return result


def check_freshness(df: pd.DataFrame, date_column: str, max_age_days: int = 7,
                     current_date: Optional[datetime] = None, level: str = "WARNING") -> dict:
    """Проверка: свежесть данных (не старше N дней)"""
    if current_date is None:
        current_date = datetime.now()
    
    max_date = df[date_column].max()
    age_days = (current_date - max_date).days
    
    result = {
        "name": f"freshness_{date_column}",
        "level": level,
        "description": f"Данные не должны быть старше {max_age_days} дней",
        "status": "PASS",
        "details": "",
        "rows_affected": None
    }
    
    if age_days > max_age_days:
        result["status"] = level
        result["details"] = f"Последние данные от {max_date.date()}, прошло {age_days} дней (максимум {max_age_days})"
    else:
        result["details"] = f"Данные актуальны. Последняя запись: {max_date.date()}, прошло {age_days} дней"
    
    return result


# ========== СПЕЦИАЛИЗИРОВАННЫЕ ПРОВЕРКИ ДЛЯ NORMALIZED ==========

def check_normalized(df: pd.DataFrame) -> List[dict]:
    """Все проверки для normalized слоя"""
    results = []
    
    results.append(check_non_empty(df, min_rows=1, level="FAIL"))
    results.append(check_not_null(df, "event_id", level="FAIL"))
    results.append(check_unique_key(df, ["event_id"], level="FAIL"))
    results.append(check_numeric_range(df, "mag", min_value=0, max_value=10, level="WARNING"))
    results.append(check_positive(df, "depth_km", level="WARNING"))
    results.append(check_not_null(df, "ts", level="FAIL"))
    
    return results


# ========== СПЕЦИАЛИЗИРОВАННЫЕ ПРОВЕРКИ ДЛЯ MART ==========

def check_mart(df: pd.DataFrame) -> List[dict]:
    """Все проверки для mart слоя"""
    results = []
    
    results.append(check_non_empty(df, min_rows=1, level="FAIL"))
    results.append(check_unique_key(df, ["date", "region_id"], level="FAIL"))
    results.append(check_not_null(df, "date", level="FAIL"))
    results.append(check_not_null(df, "region_id", level="FAIL"))
    results.append(check_positive(df, "cnt_events", level="WARNING"))
    results.append(check_allowed_values(df, "region_id", ["JP_HON"], level="FAIL"))
    
    return results


# ========== ЗАГРУЗКА ДАННЫХ ==========

def load_normalized(data_dir: Path) -> pd.DataFrame:
    """Загрузка последнего normalized файла"""
    file_path = get_latest_file(data_dir, "normalized", "normalized_*.csv")
    df = pd.read_csv(file_path, parse_dates=['ts'])
    print(f"Loaded normalized: {file_path.name}, rows={len(df)}")
    return df


def load_mart(data_dir: Path) -> pd.DataFrame:
    """Загрузка последнего mart файла"""
    file_path = get_latest_file(data_dir, "mart", "mart_daily_*.csv")
    df = pd.read_csv(file_path, parse_dates=['date'])
    print(f"Loaded mart: {file_path.name}, rows={len(df)}")
    return df


# ========== ЗАПУСК ВСЕХ ПРОВЕРОК ==========

def run_dq_for_layer(df: pd.DataFrame, layer: str) -> List[dict]:
    """Запуск DQ-проверок для конкретного слоя"""
    if layer == "normalized":
        return check_normalized(df)
    elif layer == "mart":
        return check_mart(df)
    else:
        raise ValueError(f"Unknown layer: {layer}")


def run_all_dq(data_dir: Path, layers: List[str] = ["normalized", "mart"]) -> List[dict]:
    """Запуск DQ-проверок для всех слоёв"""
    all_results = []
    
    for layer in layers:
        print(f"\n{'='*50}")
        print(f"Checking {layer.upper()} layer...")
        print(f"{'='*50}")
        
        try:
            if layer == "normalized":
                df = load_normalized(data_dir)
            else:
                df = load_mart(data_dir)
            
            results = run_dq_for_layer(df, layer)
            all_results.extend(results)
            
            for r in results:
                if r["status"] == "PASS":
                    status_icon = "[PASS]"
                elif r["status"] == "FAIL":
                    status_icon = "[FAIL]"
                else:
                    status_icon = "[WARN]"
                print(f"{status_icon} {r['name']}: {r['details']}")
                
        except FileNotFoundError as e:
            print(f"[ERROR] {e}")
    
    return all_results


def print_summary(results: List[dict]):
    """Печать результатов"""
    print("\n" + "="*50)
    print("SUMMARY")
    print("="*50)
    
    total = len(results)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    warnings = sum(1 for r in results if r["status"] == "WARNING")
    
    print(f"Total checks: {total}")
    print(f"  PASS: {passed}")
    print(f"  FAIL: {failed}")
    print(f"  WARNING: {warnings}")
    
    if failed > 0:
        print("\nFAILED checks:")
        for r in results:
            if r["status"] == "FAIL":
                print(f"  - {r['name']}: {r['details']}")
    
    if warnings > 0:
        print("\nWARNINGS:")
        for r in results:
            if r["status"] == "WARNING":
                print(f"  - {r['name']}: {r['details']}")


def save_report(results: List[dict], output_path: Path) -> None:
    """Сохранение отчёта в JSON"""
    
    report = {
        "summary": {
            "total_checks": len(results),
            "passed": sum(1 for r in results if r["status"] == "PASS"),
            "failed": sum(1 for r in results if r["status"] == "FAIL"),
            "warnings": sum(1 for r in results if r["status"] == "WARNING"),
        },
        "checks": results,
        "executed_at": datetime.now().isoformat()
    }
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    
    print(f"\nReport saved to: {output_path}")


# ========== ОСНОВНАЯ ФУНКЦИЯ ==========

def main():
    data_dir = Path(__file__).parent.parent / "data/dq"
    results = run_all_dq(data_dir, layers=["normalized", "mart"])
    print_summary(results)
    save_report(results, data_dir / "dq_report.json")


if __name__ == "__main__":
    main()
