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
