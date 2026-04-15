import pytest
import pandas as pd
from pathlib import Path
import sys

# Добавляем корневую папку проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.dq import (
    check_non_empty,
    check_not_null,
    check_unique_key,
    check_numeric_range,
    check_positive,
    check_allowed_values,
    check_normalized,
    check_mart
)


# ========== ТЕСТОВЫЕ ДАННЫЕ ==========

def good_normalized_data():
    """Хорошие данные для normalized слоя"""
    return pd.DataFrame({
        'event_id': ['A1', 'A2', 'A3'],
        'ts': pd.to_datetime(['2026-04-01', '2026-04-02', '2026-04-03']),
        'mag': [4.5, 5.0, 4.8],
        'depth_km': [50, 60, 70],
        'place': ['Place1', 'Place2', 'Place3'],
        'lat': [35.0, 36.0, 37.0],
        'lon': [135.0, 136.0, 137.0],
        'region_id': ['JP_HON', 'JP_HON', 'JP_HON'],
        'region_name': ['Япония', 'Япония', 'Япония']
    })


def bad_normalized_data():
    """Плохие данные для normalized слоя"""
    return pd.DataFrame({
        'event_id': ['A1', None, 'A1'],
        'ts': pd.to_datetime(['2026-04-01', '2026-04-02', '2026-04-03']),
        'mag': [4.5, 15.0, 4.8],
        'depth_km': [50, -10, 70],
        'place': ['Place1', 'Place2', 'Place3'],
        'lat': [35.0, 36.0, 37.0],
        'lon': [135.0, 136.0, 137.0],
        'region_id': ['JP_HON', 'JP_HON', 'JP_HON'],
        'region_name': ['Япония', 'Япония', 'Япония']
    })


def empty_normalized_data():
    """Пустые данные"""
    return pd.DataFrame(columns=['event_id', 'ts', 'mag', 'depth_km', 'place'])


def good_mart_data():
    """Хорошие данные для mart слоя"""
    return pd.DataFrame({
        'date': pd.to_datetime(['2026-04-01', '2026-04-02']),
        'cnt_events': [3, 5],
        'avg_mag': [4.5, 4.6],
        'max_mag_day': [5.0, 5.5],
        'region_id': ['JP_HON', 'JP_HON'],
        'region_name': ['Япония', 'Япония']
    })


def bad_mart_data():
    """Плохие данные для mart слоя"""
    return pd.DataFrame({
        'date': [None, pd.to_datetime('2026-04-02')],
        'cnt_events': [3, -1],
        'avg_mag': [4.5, 4.6],
        'max_mag_day': [5.0, 5.5],
        'region_id': ['JP_HON', None],
        'region_name': ['Япония', 'Япония']
    })


# ========== ПОЗИТИВНЫЕ ТЕСТЫ ==========

def test_positive_non_empty():
    """Позитивный тест: непустая таблица"""
    df = good_normalized_data()
    result = check_non_empty(df, min_rows=1)
    assert result["status"] == "PASS"
    assert result["rows_affected"] == 3


def test_positive_not_null():
    """Позитивный тест: нет NULL значений"""
    df = good_normalized_data()
    result = check_not_null(df, "event_id")
    assert result["status"] == "PASS"
    assert result["rows_affected"] == 0


def test_positive_unique_key():
    """Позитивный тест: уникальные ключи"""
    df = good_normalized_data()
    result = check_unique_key(df, ["event_id"])
    assert result["status"] == "PASS"
    assert result["rows_affected"] == 0


def test_positive_numeric_range():
    """Позитивный тест: значения в диапазоне"""
    df = good_normalized_data()
    result = check_numeric_range(df, "mag", 0, 10)
    assert result["status"] == "PASS"


def test_positive_positive():
    """Позитивный тест: положительные значения"""
    df = good_normalized_data()
    result = check_positive(df, "depth_km")
    assert result["status"] == "PASS"


# ========== НЕГАТИВНЫЕ ТЕСТЫ ==========

def test_negative_not_null():
    """Негативный тест: есть NULL значения"""
    df = bad_normalized_data()
    result = check_not_null(df, "event_id")
    assert result["status"] == "FAIL"
    assert result["rows_affected"] == 1


def test_negative_unique_key():
    """Негативный тест: есть дубликаты"""
    df = bad_normalized_data()
    result = check_unique_key(df, ["event_id"])
    assert result["status"] == "FAIL"
    assert result["rows_affected"] == 1


def test_negative_numeric_range():
    """Негативный тест: значения вне диапазона"""
    df = bad_normalized_data()
    result = check_numeric_range(df, "mag", 0, 10)
    assert result["status"] == "WARNING"
    assert result["rows_affected"] == 1


def test_negative_positive():
    """Негативный тест: отрицательные значения"""
    df = bad_normalized_data()
    result = check_positive(df, "depth_km")
    assert result["status"] == "WARNING"
    assert result["rows_affected"] == 1


# ========== ГРАНИЧНЫЕ ТЕСТЫ ==========

def test_boundary_empty_dataframe():
    """Граничный тест: пустой DataFrame"""
    df = empty_normalized_data()
    result = check_non_empty(df, min_rows=1)
    assert result["status"] == "FAIL"
    assert result["rows_affected"] == 0


def test_boundary_magnitude_at_edges():
    """Граничный тест: магнитуда на границах диапазона"""
    edge_data = pd.DataFrame({
        'event_id': ['E1', 'E2'],
        'mag': [0.0, 10.0],
        'ts': pd.to_datetime(['2026-04-01', '2026-04-02']),
        'depth_km': [50, 50],
        'place': ['P1', 'P2'],
        'lat': [35.0, 36.0],
        'lon': [135.0, 136.0],
        'region_id': ['JP_HON', 'JP_HON'],
        'region_name': ['Япония', 'Япония']
    })
    result = check_numeric_range(edge_data, "mag", 0, 10)
    assert result["status"] == "PASS"


def test_boundary_depth_zero():
    """Граничный тест: глубина = 0"""
    edge_data = pd.DataFrame({
        'event_id': ['E1'],
        'depth_km': [0],
        'ts': pd.to_datetime(['2026-04-01']),
        'mag': [4.5],
        'place': ['P1'],
        'lat': [35.0],
        'lon': [135.0],
        'region_id': ['JP_HON'],
        'region_name': ['Япония']
    })
    result = check_positive(edge_data, "depth_km")
    assert result["status"] == "WARNING"  # 0 не положительное


def test_boundary_single_row():
    """Граничный тест: одна строка"""
    single_row = good_normalized_data().head(1)
    result = check_non_empty(single_row, min_rows=1)
    assert result["status"] == "PASS"
    assert result["rows_affected"] == 1


# ========== ТЕСТЫ ДЛЯ MART СЛОЯ ==========

def test_mart_positive():
    """Позитивный тест: хорошие mart данные"""
    df = good_mart_data()
    results = check_mart(df)
    
    # Проверяем, что все проверки прошли
    for r in results:
        assert r["status"] == "PASS", f"Failed: {r['name']}"


def test_mart_negative():
    """Негативный тест: плохие mart данные"""
    df = bad_mart_data()
    results = check_mart(df)
    
    # Должны быть FAIL или WARNING
    statuses = [r["status"] for r in results]
    assert "FAIL" in statuses or "WARNING" in statuses


def test_mart_business_key_unique():
    """Тест: уникальность бизнес-ключа"""
    df = good_mart_data()
    result = check_unique_key(df, ["date", "region_id"])
    assert result["status"] == "PASS"


# ========== ТЕСТЫ ДЛЯ NORMALIZED СЛОЯ ==========

def test_normalized_all_checks():
    """Тест: все проверки normalized слоя"""
    df = good_normalized_data()
    results = check_normalized(df)
    
    # Проверяем количество проверок (6 штук)
    assert len(results) == 6
    
    # Все должны быть PASS
    for r in results:
        assert r["status"] == "PASS", f"Failed: {r['name']}"


def test_normalized_catches_errors():
    """Тест: проверки ловят ошибки в normalized"""
    df = bad_normalized_data()
    results = check_normalized(df)
    
    # Должны быть ошибки
    statuses = [r["status"] for r in results]
    assert "FAIL" in statuses or "WARNING" in statuses


# ========== ЗАПУСК ТЕСТОВ ==========

if __name__ == "__main__":
    pytest.main([__file__, "-v"])