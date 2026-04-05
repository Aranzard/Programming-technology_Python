-- 1. Таблица не пуста
SELECT COUNT(*) AS row_count
FROM mart_earthquake_jp;

-- 2. Диапазон дат
SELECT MIN(date) AS min_date,
       MAX(date) AS max_date
FROM mart_earthquake_jp;

-- 3. NULL в ключевых колонках
SELECT
    SUM(CASE WHEN date IS NULL THEN 1 ELSE 0 END) AS null_date,
    SUM(CASE WHEN region_id IS NULL THEN 1 ELSE 0 END) AS null_region_id,
    SUM(CASE WHEN cnt_events IS NULL THEN 1 ELSE 0 END) AS null_cnt_events
FROM mart_earthquake_jp;

-- 4. Дубли по бизнес-ключу (день + регион)
SELECT date, region_id, COUNT(*) AS cnt
FROM mart_earthquake_jp
GROUP BY date, region_id
HAVING COUNT(*) > 1;

-- 5. KPI-проверка (регион + метрики)
SELECT region_id,
       region_name,
       SUM(cnt_events) AS total_events,
       AVG(avg_mag) AS avg_magnitude,
       MAX(max_mag_day) AS max_magnitude,
       AVG(deep_events_percent) AS avg_deep_percent
FROM mart_earthquake_jp
GROUP BY region_id, region_name;