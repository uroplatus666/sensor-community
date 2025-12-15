import logging
import sys
import os
import json
import datetime
from datetime import timedelta, timezone

from scraper import scrape_data
from processor import run_processing
from uploader import run_upload

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)


def load_config():
    with open('app/config.json', 'r') as f:
        config = json.load(f)
    env_token = os.getenv('MAPBOX_TOKEN')
    if env_token:
        config['mapbox_token'] = env_token
    return config


# --- РАБОТА СО STATE-ФАЙЛОМ ---

def get_state_file_path(config):
    """Возвращает путь к файлу состояния внутри папки data"""
    data_dir = config.get('data_dir', 'data')
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, 'state.json')


def load_state(state_path):
    """Читает состояние из JSON файла."""
    if not os.path.exists(state_path):
        return {}
    try:
        with open(state_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logging.warning(f"Failed to load state file {state_path}: {e}. Starting clean.")
        return {}


def save_state(state_path, state_data):
    """Сохраняет состояние в JSON файл."""
    try:
        with open(state_path, 'w', encoding='utf-8') as f:
            json.dump(state_data, f, indent=4, ensure_ascii=False)
        logging.info(f"💾 State saved to {state_path}")
    except Exception as e:
        logging.error(f"Failed to save state file: {e}")


# --- ЛОГИКА РАСЧЕТА ДАТ ---

def prepare_schedule_and_state(config, current_state):
    """
    Рассчитывает target_start на основе state.json.
    """
    today = datetime.datetime.now(timezone.utc).date()
    today_str = today.strftime("%Y-%m-%d")

    logging.info(f"📅 Daily Job: Today is {today_str}")

    sensor_types = ['sds', 'bme']

    # Готовим объект будущего состояния
    new_state = current_state.copy()
    at_least_one_task = False

    for s_type in sensor_types:
        if s_type not in config.get('sensors', {}):
            continue

        if s_type not in new_state:
            new_state[s_type] = {}

        sensors = config['sensors'][s_type]

        for sensor_id, dates in sensors.items():
            sensor_id_str = str(sensor_id)

            # --- 1. Определение точки старта ---

            # Дата по умолчанию из конфига
            try:
                config_start_dt = datetime.datetime.strptime(dates['start'], "%Y-%m-%d").date()
            except ValueError:
                config_start_dt = today

            # Проверяем State
            sensor_state = new_state[s_type].get(sensor_id_str, {})
            last_downloaded_str = sensor_state.get('last_downloaded')

            target_start = config_start_dt

            if last_downloaded_str:
                # ВАЖНО: Если данные были скачаны, начинаем со следующего дня
                try:
                    last_dt = datetime.datetime.strptime(last_downloaded_str, "%Y-%m-%d").date()
                    target_start = last_dt + timedelta(days=1)
                    logging.info(
                        f"Sensor {sensor_id}: Resuming from state. Last downloaded: {last_dt}. Next start: {target_start}")
                except ValueError:
                    logging.warning(f"Sensor {sensor_id}: Corrupted date in state. Using config start.")
            else:
                logging.info(f"Sensor {sensor_id}: No history in state. Starting from config: {config_start_dt}")
                sensor_state['initial_start'] = config_start_dt.strftime("%Y-%m-%d")

            # Защита от дат в будущем
            if target_start > today:
                logging.info(
                    f"Sensor {sensor_id}: Data is up to date (Target {target_start} > Today). Skipping scrape.")
                # Ставим даты так, чтобы скрапер ничего не делал (start > end)
                config['sensors'][s_type][sensor_id]['start'] = (today + timedelta(days=1)).strftime("%Y-%m-%d")
                config['sensors'][s_type][sensor_id]['end'] = today_str
            else:
                # Нормальный режим
                config['sensors'][s_type][sensor_id]['start'] = target_start.strftime("%Y-%m-%d")
                config['sensors'][s_type][sensor_id]['end'] = today_str
                at_least_one_task = True
                logging.info(
                    f"   👉 Plan for {sensor_id}: {config['sensors'][s_type][sensor_id]['start']} -> {today_str}")

            # --- 2. Обновляем состояние (предварительно) ---
            # Считаем, что если скрапер отработает, то мы скачаем все вплоть до 'today_str'
            sensor_state['last_downloaded'] = today_str
            sensor_state['last_run_timestamp'] = datetime.datetime.now().isoformat()

            new_state[s_type][sensor_id_str] = sensor_state

    return config, new_state, at_least_one_task


def main():
    logging.info("🚀 Job started.")
    try:
        # 1. Загрузка
        config = load_config()
        state_path = get_state_file_path(config)
        current_state = load_state(state_path)

        # 2. Расчет
        config, pending_state, has_tasks = prepare_schedule_and_state(config, current_state)

        # 3. ETL Пайплайн

        # --- A. SCRAPING ---
        if has_tasks:
            scrape_data(config)
            # ВАЖНО: Сохраняем стейт СРАЗУ после успешного скачивания.
            # Даже если процессинг упадет, мы запомним, что файлы уже у нас.
            save_state(state_path, pending_state)
        else:
            logging.info("💤 Skipping scrape (everything up to date).")

        # --- B. PROCESSING ---
        # Процессор работает с локальными файлами, он найдет всё, что есть в папке
        run_processing(config)

        # --- C. UPLOADING ---
        # Аплоадер сам проверит сервер на дубликаты
        run_upload(config)

        logging.info("✅ Job finished successfully.")

    except Exception as e:
        logging.critical(f"🔥 Job failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()