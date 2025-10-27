## Парсинг данных сенсоров с `https://archive.sensor.community/` и загрузка на FrostServer


```
.
├── sds_scrape.py      # Парсинг данных SDS011 (пыль)
├── bme_scrape.py      # Парсинг данных BME280 (температура, давление, влажность)
├── process.py         # Обработка и агрегирование
├── load_frost.py      # Загрузка данных на FrostServer
├── descriptions.xlsx  # Исходные известные арактеристики датчиков
├── FrostServer
│ └── compose.yml      # Образ для создания локального FrostServer
├── example            # Дириктория-пример для загрузки уже выгруженных данных на FrostServer
│ ├── data 
| │ ├── SDS011         # Папка с файлами сенсора типа SDS011
| │ │ └── 82312        # Папка с csv файлами для данного sensor-id
| │ └── BME280         # Папка с файлами сенсора типа BME280
| │    └── 82313       # Папка с csv файлами для данного sensor-id
| ├── load_frost.py    # Загрузка данных на FrostServer
│ └── all_stats.xlsx   # Агрегированные характеристики датчиков, их локаций и т.д.
└── README.md          # Вы читаете этот файл
```
`sensor_ids = {
    "sensor_id":
        {
        "start_date": "year-month-day", "end_date": "year-month-day"
        },
    ...
}`
### Запуск сервиса
0. Клонируйте репозиторий
```bash
git clone https://github.com/uroplatus666/sensor-community.git
```
1. Ввод своих данных

1.1. В файлах:
- `sds_scrape.py`
- `bme_scrape.py`
- `load_frost.py`
вставьте нужные sensor-id и диапазоны дат:
`sensor_ids = {
    "sensor_id":
       {
    "start_date": "year-month-day", "end_date": "year-month-day"
       },
    ...
}`

1.2. В файле `process.py` вставьте свой `MAPBOX_TOKEN` для геокодирования координат
  
1.3. В файле `load_frost.py` вставьте свой 'BASE_URL' - адрес FrostServer или поднимите его локально, для этого откройте новый терминал в дириктории `FrostServer`:
```bash
wsl
docker-compose up -d
```

2. Парсим данные
- Создаем виртуальную среду
```bash
python -m venv venv
venv\Scripts\Activate.ps1
pip install requests pandas openpyxl
```
- Парсим данные с пылевых датчиков SDS011
```bash
python sds_scrape.py
```
- Парсим данные с датчиков, измеряющих температуру, влажность, давление BME280
```bash
python bme_scrape.py
```
3. Обрабатываем данные

3.1. Смотрим статистику по скаченным файлам

3.2. Создаем общий файл со следующими данными
- `sensor_type`                    Тип датчика (BME280, SDS011)
- `sensor_id`
- `days`                           Количество дней, в которые датчик измерял данные и они есть в архиве
- `lat`       
- `lon`
- `first_seen`                     Дата и время певрого измерения этого датчика на данной локации
- `last_seen`                      Дата и время последнег измерения этого датчика на данной локации
- `address`                        Адрес, геокодированный с lat, lon с помощью Mapbox
- `Инвентарный номер изделия`      Получены после агрегации характеристик датчиков с `descriptions.xlsx`
- `Марка`                          Получены после агрегации характеристик датчиков с `descriptions.xlsx`
- `Номер процессора`               Получены после агрегации характеристик датчиков с `descriptions.xlsx`
- `Тип`                            Получены после агрегации характеристик датчиков с `descriptions.xlsx`
```bash
python process.py
```
4. Выгружаем данные на FrostServer
```bash
python frost_load.py
```

#### BONUS
Загрузка тестовых данных на FrostServer без скачивания
```bash
cd example
python -m venv venv
venv\Scripts\Activate.ps1
pip install requests pandas openpyxl
python frost_load.py
```
#### Если вы запускали локальный FrostServer и вам требуется:
- Быстро избавиться от всех данных, то сделайте это:
```bash
docker compose down
docker volume ls | grep postgis
docker volume rm test_sending_postgis_volume
docker compose up -d
```
- Остановить текущие контейнеры и удалить их:
```bash
docker stop $(docker ps -q)
docker rm $(docker ps -aq)
```
