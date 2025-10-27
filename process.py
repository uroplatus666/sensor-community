import os, time, math, random, requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, Dict, List

# Ключи
MAPBOX_TOKEN = "YOUR_TOKEN" # ← ВАШ токен

#______________________Вывод статистика по скаченным данным_____________________

def scan_dir(root_path, label):
    print(f'\n🚀 {label}  ({root_path})')
    if not os.path.isdir(root_path):
        print('   ❌ директория не найдена')
        return

    # только под-папки 1-го уровня
    for entry in sorted(os.listdir(root_path)):
        sub = os.path.join(root_path, entry)
        if not os.path.isdir(sub):
            continue

        files = [f for f in os.listdir(sub) if os.path.isfile(os.path.join(sub, f))]
        # Подсчет суммарного количества строк во всех файлах
        total_lines = 0
        for f in files:
            file_path = os.path.join(sub, f)
            try:
                with open(file_path, 'r', encoding='utf-8') as file:
                    total_lines += sum(1 for _ in file)
            except (IOError, UnicodeDecodeError):
                print(f'        ⚠️ Не удалось прочитать файл: {f}')

        print(f'   📁 {entry}/  →  {len(files)} файл(ов), суммарная длина: {total_lines} строк')
        if files:
            print(f'        ✨пример: {files[0]}')

folder_sds = r'data\SDS011'
folder_bme = r'data\BME280'

scan_dir(folder_sds, 'SDS011')
scan_dir(folder_bme, 'BME280')

# ______________________Создание общего файла________________________

def _read_sensor_csv(path):
    last_err = None
    try:
        df = pd.read_csv(path, sep=';')
        sub = df[['timestamp', 'lat', 'lon']].copy()

        # приводим типы (учитываем возможные десятичные запятые)
        sub['lat'] = pd.to_numeric(sub['lat'].astype(str).str.replace(',', '.'), errors='coerce')
        sub['lon'] = pd.to_numeric(sub['lon'].astype(str).str.replace(',', '.'), errors='coerce')
        sub['timestamp'] = pd.to_datetime(sub['timestamp'], errors='coerce', utc=False)

        sub = sub.dropna(subset=['timestamp', 'lat', 'lon'])

        return sub  # успех
    except Exception as e:
        last_err = f'{type(e).__name__}: {e}'

    # если ничего не подошло — даём понять вызывающему коду
    raise RuntimeError(f'Не удалось прочитать {os.path.basename(path)}. Последняя ошибка: {last_err}')

def process_root(root_path, sensor_type):
    """Собирает строки результата для одной корневой папки типа датчика."""
    rows = []
    if not os.path.isdir(root_path):
        return rows

    for entry in sorted(os.listdir(root_path)):
        sub = os.path.join(root_path, entry)
        if not os.path.isdir(sub):
            continue

        sensor_id = entry.strip()
        csv_files = sorted([f for f in os.listdir(sub) if f.lower().endswith('.csv')])
        days_count = len(csv_files)  # по условию = количеству файлов
        if days_count == 0:
            continue

        # границы появления по каждой точной паре (lat, lon)
        loc_bounds = {}  # (lat, lon) -> [min_ts, max_ts]

        for fname in csv_files:
            fpath = os.path.join(sub, fname)
            try:
                df = _read_sensor_csv(fpath)
            except Exception:
                # пропускаем проблемные файлы, чтобы не ронять процесс
                continue

            g = df.groupby(['lat', 'lon'])['timestamp'].agg(['min', 'max']).reset_index()
            for _, r in g.iterrows():
                key = (float(r['lat']), float(r['lon']))
                mn, mx = pd.Timestamp(r['min']), pd.Timestamp(r['max'])
                if key not in loc_bounds:
                    loc_bounds[key] = [mn, mx]
                else:
                    if pd.notna(mn) and (pd.isna(loc_bounds[key][0]) or mn < loc_bounds[key][0]):
                        loc_bounds[key][0] = mn
                    if pd.notna(mx) and (pd.isna(loc_bounds[key][1]) or mx > loc_bounds[key][1]):
                        loc_bounds[key][1] = mx

        # формируем строки результата по всем локациям датчика
        for (lat, lon), (mn, mx) in loc_bounds.items():
            rows.append({
                'sensor_type': sensor_type,                           # ВВОД: тип датчика
                'sensor_id': sensor_id,                               # ВВОД: номер датчика (имя подпапки)
                'days': days_count,                                   # ВЫЧИСЛЕНО: кол-во файлов в папке
                'lat': lat,                                           # ИЗ CSV: точный lat
                'lon': lon,                                           # ИЗ CSV: точный lon
                'first_seen': mn.strftime('%Y-%m-%dT%H:%M:%S') if pd.notna(mn) else None,
                'last_seen':  mx.strftime('%Y-%m-%dT%H:%M:%S') if pd.notna(mx) else None,
            })

    return rows

# Запуск обработки и сохранение результата
all_rows = []
all_rows.extend(process_root(folder_sds, 'SDS011'))
all_rows.extend(process_root(folder_bme, 'BME280'))
df = pd.DataFrame(all_rows)
output_xlsx = 'archive_stats.xlsx'  # ВЫВОД: имя итогового файла

if df.empty:
    print('⚠️ Итоговая таблица пуста. Проверьте пути и содержимое.')
else:
    df = df.sort_values(['sensor_type', 'sensor_id', 'first_seen', 'lat', 'lon']).reset_index(drop=True)
    df.to_excel(output_xlsx, index=False)
    print(f'✅ Готово: {output_xlsx}  |  строк: {len(df)}')
    
# ____________________Добавление адреса_______________________

MAPBOX_ENDPOINT = "https://api.mapbox.com/geocoding/v5/mapbox.places/{lon},{lat}.json"
DEFAULT_THREADS = 8
MAX_RETRIES     = 5
TIMEOUT_SEC     = 12

# Фолбэки типов: от более узких к более широким
FALLBACK_TYPES: List[Optional[str]] = [
    "address,street,neighborhood,locality,place,region,postcode,poi",  # базовый
    "address,street,place,region,postcode",                             # усечённый
    None                                                                # без фильтра типов
]

def _mk_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": "mapbox-revgeo-ru/1.2"})
    return s

def _sleep_backoff(attempt: int, retry_after: Optional[str] = None) -> None:
    if retry_after:
        try:
            time.sleep(min(60.0, float(retry_after))); return
        except ValueError:
            pass
    time.sleep(0.5 * (2 ** attempt) + random.uniform(0, 0.25))

def _coerce_float(x):
    try:
        if x is None: return None
        if isinstance(x, (float, int)): return float(x)
        return float(str(x).strip().replace(",", "."))
    except Exception:
        return None

def _looks_swapped(lat: Optional[float], lon: Optional[float]) -> bool:
    # Для РФ: широта обычно ~40..82, долгота ~19..180 ИЛИ ~-180..-169 (пересечение 180-го меридиана)
    if lat is None or lon is None: 
        return False
    lat_ok = 40.0 <= lat <= 82.0
    lon_ok = (19.0 <= lon <= 180.0) or (-180.0 <= lon <= -169.0)
    # Если lat похож на долготу РФ, а lon похож на широту РФ — вероятно, поменяли местами
    lat_like_lon = (19.0 <= lat <= 180.0) or (-180.0 <= lat <= -169.0)
    lon_like_lat = 40.0 <= lon <= 82.0
    return (not lat_ok and not lon_ok) and (lat_like_lon and lon_like_lat)

def _reverse_once(session: requests.Session, token: str, lon: float, lat: float,
                  *, language: str, country: Optional[str], types: Optional[str]) -> Optional[str]:
    url = MAPBOX_ENDPOINT.format(lon=str(lon), lat=str(lat))
    params = {
        "access_token": token,
        "language": language,
        "limit": 1,
    }
    if country:
        params["country"] = country
    if types:
        params["types"] = types

    for attempt in range(MAX_RETRIES):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT_SEC)
            if r.status_code == 200:
                data = r.json()
                feats = data.get("features") or []
                return feats[0].get("place_name") if feats else None
            if r.status_code in (429, 500, 502, 503, 504):
                _sleep_backoff(attempt, r.headers.get("Retry-After")); continue
            if r.status_code in (401, 403):
                raise RuntimeError(f"Mapbox auth error {r.status_code}: {r.text[:200]}")
            if r.status_code in (400, 404, 422):
                return None
            _sleep_backoff(attempt)
        except requests.RequestException:
            _sleep_backoff(attempt)
    return None

def reverse_geocode_point(token: str, lon: float, lat: float,
                          *, language: str = "ru", country: Optional[str] = "ru") -> Optional[str]:
    """Один вызов с каскадом фолбэков: types → без types; country → без country."""
    session = _mk_session()
    # 1) types с country
    for t in FALLBACK_TYPES:
        addr = _reverse_once(session, token, lon, lat, language=language, country=country, types=t)
        if addr: 
            return addr
    # 2) снять country (иногда граничные точки)
    for t in FALLBACK_TYPES:
        addr = _reverse_once(session, token, lon, lat, language=language, country=None, types=t)
        if addr:
            return addr
    # 3) микро-смещение (на случай попадания в «ничейный» тайл/полигон)
    for dx, dy in ((1e-4,0),(-1e-4,0),(0,1e-4),(0,-1e-4)):
        for t in FALLBACK_TYPES:
            addr = _reverse_once(session, token, lon+dx, lat+dy, language=language, country=country, types=t)
            if addr:
                return addr
    return None

def _preflight(token: str) -> None:
    """Быстрая проверка токена на координатах Кремля; бросает исключение при проблеме."""
    addr = reverse_geocode_point(token, 37.6175, 55.7520, country="ru")
    if not addr:
        raise RuntimeError("Preflight: не получили адрес по тестовой точке (Кремль). "
                           "Проверьте права токена на Geocoding и сетевой доступ к api.mapbox.com.")

def reverse_geocode_mapbox_bulk(
    df: pd.DataFrame,
    *,
    token: Optional[str] = None,
    lat_col: str = "lat",
    lon_col: str = "lon",
    threads: int = DEFAULT_THREADS,
    language: str = "ru",
    country: Optional[str] = "ru",
    autoswap: bool = True,
    do_preflight: bool = True
) -> pd.Series:

    if token is None:
        token = os.getenv("MAPBOX_TOKEN")
    if not token:
        raise ValueError("Нет токена. Передайте token=... или установите переменную окружения MAPBOX_TOKEN.")

    if lat_col not in df.columns or lon_col not in df.columns:
        raise ValueError(f"В df нет колонок '{lat_col}' и/или '{lon_col}'.")

    lats = df[lat_col].map(_coerce_float)
    lons = df[lon_col].map(_coerce_float)

    if do_preflight:
        _preflight(token)

    # Подготовим возможную перестановку для каждой точки
    coords = []
    for lat, lon in zip(lats, lons):
        if autoswap and _looks_swapped(lat, lon):
            coords.append((lat, lon, True))   # отмечаем как «перепутано»
        else:
            coords.append((lat, lon, False))

    session = _mk_session()
    cache: Dict[Tuple[float, float], Optional[str]] = {}
    results = [None] * len(df)
    idx_map = list(df.index)

    def _resolve_one(lon: float, lat: float) -> Optional[str]:
        # Каскад фолбэков (быстрый путь без префлайта и без лишних сессий)
        # 1) types + country
        for t in FALLBACK_TYPES:
            a = _reverse_once(session, token, lon, lat, language=language, country=country, types=t)
            if a: return a
        # 2) без country
        for t in FALLBACK_TYPES:
            a = _reverse_once(session, token, lon, lat, language=language, country=None, types=t)
            if a: return a
        # 3) микро-смещения
        for dx, dy in ((1e-4,0),(-1e-4,0),(0,1e-4),(0,-1e-4)):
            for t in FALLBACK_TYPES:
                a = _reverse_once(session, token, lon+dx, lat+dy, language=language, country=country, types=t)
                if a: return a
        return None

    futures = {}
    with ThreadPoolExecutor(max_workers=max(1, int(threads))) as ex:
        for i, (lat, lon, swapped) in enumerate(coords):
            if lat is None or lon is None or (isinstance(lat, float) and math.isnan(lat)) or (isinstance(lon, float) and math.isnan(lon)):
                results[i] = None; continue

            # если эвристика сказала «перепутано» — меняем местами
            use_lat, use_lon = (lon, lat) if swapped else (lat, lon)

            # Mapbox ждёт порядок lon,lat
            key = (use_lon, use_lat)
            if key in cache:
                results[i] = cache[key]; continue
            fut = ex.submit(_resolve_one, use_lon, use_lat)
            futures[fut] = (i, key)

        for fut in as_completed(futures):
            i, key = futures[fut]
            try:
                addr = fut.result()
            except Exception:
                addr = None
            cache[key] = addr
            results[i] = addr

    return pd.Series(results, index=idx_map, name="address_ru")


df["address"] = reverse_geocode_mapbox_bulk(
    df, token=MAPBOX_TOKEN, lat_col="lat", lon_col="lon", threads=8
)
df.to_excel('archive_stats.xlsx', index = False)

# _______________________Добавление других характеристик_____________________

description = pd.read_excel('description.xlsx')
d1 = description[['Инвентарный номер изделия', 'Тип', 'Марка',
             'Номер процессора', 'SDS011']]
d1 = d1.rename(columns = {'SDS011': 'sensor_id'}).dropna()

d2 = description[['Инвентарный номер изделия', 'Тип', 'Марка',
             'Номер процессора', 'BME280']]
d2 = d2.rename(columns = {'BME280': 'sensor_id'}).dropna()
d12 = pd.concat([d1, d2], axis = 0)

def norm_id_to_int(s: pd.Series) -> pd.Series:
    out = s.astype(str).str.extract(r'(\d+)')[0]
    out = pd.to_numeric(out, errors='coerce').astype('Int64')
    return out

df['sensor_id']  = norm_id_to_int(df['sensor_id'])
d12['sensor_id'] = norm_id_to_int(d12['sensor_id'])

all_stats= pd.merge(df, d12, how = 'left', on = 'sensor_id')
all_stats.to_excel('all_stats.xlsx', index = False)



