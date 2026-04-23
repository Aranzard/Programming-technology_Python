# Data Contract

## Источник данных
- **Вариант:** 16
- **Тема:** Землетрясения (USGS) - Япония (Хонсю)
- **Source Type:** usgs_earthquake

## Параметры подключения
- **Endpoint:** https://earthquake.usgs.gov/fdsnws/event/1/query
- **HTTP-метод:** GET
- **Параметры запроса:**
  - format: geojson
  - minmagnitude: 4.0
  - minlatitude: 30.0
  - maxlatitude: 46.0
  - minlongitude: 129.0
  - maxlongitude: 146.0
  - starttime: {YYYY-MM-DD}
  - endtime: {YYYY-MM-DD}

## Частота загрузки
- Последние 30 дней, ручной запуск

## Ограничения
- API без аутентификации
- Магнитуда: [0; 10]
- starttime/endtime обязательны


## Схема normalized 
- Grain / зерно таблицы: одна строка = одно землетрясение.
- Источник raw: data/raw/variant_XX/...json
- Результат normalizaton: data/normalized/variant_XX/...csv

schema_df = pd.DataFrame([
    {"field": "time", "dtype": "datetime64[ns]", "nullable": "yes", "description": "Время землетрясения", "source_path": "properties.time"},
    {"field": "mag", "dtype": "float32", "nullable": "yes", "description": "Магнитуда землетрясения", "source_path": "properties.mag"},
    {"field": "place", "dtype": "object", "nullable": "yes", "description": "Текстовое описание местоположения", "source_path": "properties.place"},
    {"field": "latitude", "dtype": "float32", "nullable": "yes", "description": "Широта эпицентра", "source_path": "geometry.coordinates[1]"},
    {"field": "longitude", "dtype": "float32", "nullable": "yes", "description": "Долгота эпицентра", "source_path": "geometry.coordinates[0]"},
    {"field": "depth", "dtype": "float32", "nullable": "yes", "description": "Глубина эпицентра (км)", "source_path": "geometry.coordinates[2]"},
    {"field": "alert", "dtype": "object", "nullable": "yes", "description": "Уровень оповещения (green/yellow/orange/red/no_alert)", "source_path": "properties.alert"},
    {"field": "tsunami", "dtype": "int8", "nullable": "no", "description": "Наличие цунами (1 - да, 0 - нет)", "source_path": "properties.tsunami"},
    {"field": "sig", "dtype": "int32", "nullable": "yes", "description": "Значимость события (0-1000)", "source_path": "properties.sig"},
])



week 9 
# Data Contract — Earthquake Analytics (Variant 16)

**Contract version:** 1.0 

---

## Purpose

Данный контракт описывает структуру данных, типы, единицы измерения и правила именования для пайплайна по обработке данных о землетрясениях в регионе Япония (остров Хонсю). Основная цель — обеспечить единое понимание данных.

**Бизнес-цель:** Анализ сейсмической активности, выявление аномальных дней, отслеживание частоты и силы землетрясений.

---

## Time & Timezone

| Параметр | Значение |
|----------|----------|
| **Часовой пояс** | UTC (все временные метки приведены к UTC) |
| **Поле timestamp** | `ts` (в normalized) |
| **Поле даты** | `date` (в mart) — календарная дата в UTC |

**Примечание:** Исходные данные от USGS API приходят в миллисекундах с 1970-01-01 UTC. При преобразовании сохраняется UTC.

---

## Grain (Гранулярность данных)

| Слой | Гранулярность | Описание |
|------|---------------|----------|
| **normalized** | 1 строка = 1 землетрясение | Каждое событие (earthquake) представлено отдельной записью |
| **mart** | 1 строка = 1 день + 1 регион | Агрегация всех событий за день по региону Хонсю |

**Регион анализа:** Япония, Хонсю (широта: 30°-46° с.ш., долгота: 129°-146° в.д.)

---

## Naming & Units Rules

### Общие правила именования

| Правило | Пример | Примечание |
|---------|--------|------------|
| **snake_case** | `cnt_events`, `max_mag_day` | Все колонки в нижнем регистре с подчёркиваниями |
| **KPI prefixes** | `cnt_*`, `sum_*`, `avg_*`, `min_*`, `max_*` | Явно указывают тип агрегации |
| **Даты** | `date`, `ts` | `date` — календарная дата, `ts` — timestamp |
| **Единицы** | В суффиксе или контракте | `depth_km`, `deep_events_percent` |

### Стратегия указания единиц

- **В имени колонки:** `_km`, `_percent`, `_utc`
- **В контракте:** для всех колонок указана колонка `unit`

## Schema: normalized layer

**Описание:** Очищенные данные о землетрясениях после удаления дублей и пропусков. Одна строка = одно событие.

| column | dtype | nullable | unit | description |
|--------|-------|----------|------|-------------|
| `event_id` | string | NO | - | Уникальный идентификатор события в системе USGS (например, "us6000sh9j") |
| `ts` | timestamp (UTC) | NO | - | Дата и время события в UTC |
| `mag` | float | YES | магнитуда | Магнитуда землетрясения |
| `place` | string | YES | - | Текстовое описание места события ("52 km NE of Misawa, Japan") |
| `depth_km` | float | NO | км | Глубина гипоцентра в километрах (0–700) |
| `lat` | float | NO | градусы | Широта эпицентра, диапазон: [-90, 90] |
| `lon` | float | NO | градусы | Долгота эпицентра, диапазон: [-180, 180] |
| `region_id` | string | NO | - | Идентификатор региона (фиксирован: "JP_HON") |
| `region_name` | string | NO | - | Название региона (фиксирован: "Япония (Хонсю)") |

---

## Schema: mart layer (витрина)

**Описание:** Агрегированная витрина для анализа сейсмической активности по дням.

| column | dtype | nullable | unit | description |
|--------|-------|----------|------|-------------|
| `date` | date | NO | - | Календарная дата (YYYY-MM-DD в UTC) |
| `cnt_events` | integer | NO | событий | Количество землетрясений за день (магнитуда ≥ 4.0) |
| `max_mag_day` | float | YES | магнитуда | Максимальная магнитуда среди всех событий дня |
| `avg_mag` | float | YES | магнитуда | Средняя магнитуда всех событий дня |
| `min_mag_day` | float | YES | магнитуда | Минимальная магнитуда среди всех событий дня |
| `deep_events_percent` | float | YES | % | Доля событий с глубиной > 70 км (процент от всех событий периода) |
| `total_events` | integer | NO | событий | Общее количество событий за весь период (одинаково для всех строк) |
| `max_magnitude_overall` | float | NO | магнитуда | Максимальная магнитуда за весь период (одинаково для всех строк) |
| `region_id` | string | NO | - | Идентификатор региона (фиксирован: "JP_HON") |
| `region_name` | string | NO | - | Название региона (фиксирован: "Япония (Хонсю)") |

---

## KPI Definitions

| KPI | Формула расчёта | Business meaning |
|-----|-----------------|------------------|
| `cnt_events` | `COUNT(event_id) GROUP BY date` | Интенсивность сейсмической активности в конкретный день |
| `avg_mag` | `AVG(mag) GROUP BY date` | Типичная сила землетрясений в этот день |
| `max_mag_day` | `MAX(mag) GROUP BY date` | Наиболее сильное событие дня |
| `deep_events_percent` | `AVG(depth_km > 70) * 100` | Доля глубоких событий (менее опасны для поверхности) |

