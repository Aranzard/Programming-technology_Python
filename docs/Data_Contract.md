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