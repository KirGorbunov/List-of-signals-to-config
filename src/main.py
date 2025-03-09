import logging
from src.data_loader import DataLoader, DataConstructor
from src.signal_processor import SignalProcessor
from src.data_mapper import DataMapper, ConfigGenerator
from src.file_creator import FileCreator
from src.settings import settings


logging.basicConfig(
    level=settings.LOGGING_LEVEL,
    format="{levelname} - {message}",
    style="{",
    encoding='utf-8'
)

def main():
    # Загрузка данных
    data_loader = DataLoader(settings.LIST_OF_SIGNALS_FILE)
    signals_data = data_loader.load_signals()
    devices_data = data_loader.load_devices()
    logging.info(
        f"Данные загружены: signals ({signals_data.shape[0]} строк), devices ({devices_data.shape[0]} строк)."
    )

    # Объединение данных
    merged_data = DataConstructor.merge(signals_data, devices_data)
    logging.debug("Данные сигналов и устройств объединены.")

    # Обработка сигналов:
    processor = SignalProcessor()
    filtered_signals = processor.filter_signals(merged_data)
    grouped_signals = processor.group_signals(filtered_signals)
    signals_with_concatenated_devices = processor.concatenate_devices(grouped_signals)
    normalize_signals = processor.fill_missing_data_types(signals_with_concatenated_devices)
    signals_divided_by_assets = processor.divide_by_assets(normalize_signals)

    # Создание маппингов:
    for asset, signals in signals_divided_by_assets.items():
        data_mapper = DataMapper()
        data_mapping = data_mapper.create_data_mapping(asset, signals)
        slaves_mapping = data_mapper.create_slaves_mapping(signals)
        signals_template = data_mapper.create_signals_template(signals)
        logging.debug(f"Маппинги для {asset} созданы")

        # Генерация конифга:
        config_generator = ConfigGenerator()
        config = config_generator.generate_config(data_mapping, slaves_mapping)
        logging.debug(f"Конфиг для ассета {asset} сгенерирован")

        # Создание файлов:
        file_creator = FileCreator(asset, signals_template, config)
        file_creator.create_json_with_config()
        file_creator.create_excel_data_template()
    logging.info("Все файлы созданы")


if __name__ == "__main__":
    main()
