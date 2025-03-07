import json
import logging
from typing import NoReturn

import numpy as np
import pandas as pd

from src.settings import settings

logging.basicConfig(
    level=settings.LOGGING_LEVEL,
    format="{levelname} - {message}",
    style="{",
    encoding='utf-8'
)

class DataLoader:
    """Класс для загрузки данных сигналов и устройств из Excel-файлов."""

    def __init__(self, signals_file: str):
        self.signals_file = signals_file

    def load_signals(self) -> pd.DataFrame:
        """Загружает данные сигналов из Excel-файла."""

        signals = pd.read_excel(
            self.signals_file,
            sheet_name=settings.SIGNALS_SHEET,
            usecols=[
                settings.SIGNALS_SHEET_DEVICE_COLUMN,
                settings.CODE_COLUMN,
                settings.SIGNAL_TYPE_COLUMN,
                settings.ADDRESS_COLUMN,
                settings.VALUE_TYPE_COLUMN,
                settings.ASSET_COLUMN
            ],
            dtype=str
        )
        return signals

    def load_devices(self) -> pd.DataFrame:
        """Загружает данные устройств из Excel-файла."""
        devices = pd.read_excel(
            self.signals_file,
            sheet_name=settings.DEVICES_SHEET,
            usecols=[
                settings.GATEWAY_COLUMN,
                settings.DEVICES_SHEET_DEVICE_COLUMN,
                settings.COMMON_ADDRESS_COLUMN
            ],
            dtype=str
        )
        return devices


class DataConstructor:
    """Класс для создания общего датасета данных сигналов и устройств."""

    @staticmethod
    def merge(signals: pd.DataFrame, devices: pd.DataFrame) -> pd.DataFrame:
        """
        Объединяет два датасета signals и devices по общему столбцу device.

        Параметры:
        - signals: DataFrame с данными сигналов.
        - devices: DataFrame с данными устройств.

        Возвращает:
        - DataFrame, содержащий объединенные данные сигналов и устройств.
        """

        # Переименование столбца в датасете devices для соответствия имени столбца в датасете signals
        devices_renamed = devices.rename(
            columns={settings.DEVICES_SHEET_DEVICE_COLUMN: settings.SIGNALS_SHEET_DEVICE_COLUMN}
        )

        # Объединение датасетов по общему столбцу device
        merged_data = pd.merge(
            signals,
            devices_renamed,
            on=settings.SIGNALS_SHEET_DEVICE_COLUMN,
            how="left"
        )
        return merged_data


class SignalProcessor:
    """Класс для обработки и подготовки данных сигналов."""

    @staticmethod
    def filter_signals(df: pd.DataFrame) -> pd.DataFrame:
        """
            Фильтрует DataFrame, оставляя только сигналы, которые необходимо эмулировать (проверка на тип сигнала)
            и для которых задан modbus-адрес (проверка на наличие адреса).

            Параметры:
            - df: общий DataFrame со всеми сигналами

            Возвращает:
            - DataFrame с отфильтрованными строками, содержащими только измереяемые сигналы с modbus-адресами.
            """

        return df[
            (df[settings.SIGNAL_TYPE_COLUMN] == settings.ONLY_SIGNALS_TYPE) &
            (df[settings.ADDRESS_COLUMN].notna())
            ]

    @staticmethod
    def group_signals(signals: pd.DataFrame) -> pd.DataFrame:
        """
        Группирует сигналы, относящиеся к одному регистру (количество записей для каждой комбинации
        'address' и 'common_address' > 1), и изменяет название кода сигнала ('code').
        Если в регистре только один сигнал, к коду сигнала добавляется название гейтвея.

        Параметры:
        - signals: общий DataFrame cо всеми измеряемыми сигналами

        Возвращает:
        -  DataFrame со сгруппированными сигналами и измененным столбцом 'code'.
        Если сигналы относятся к одному регистру (одинаковые 'address' и 'common_address'),
        к 'code' добавляется количество сигналов, название гейтвея и modbus-адрес.
        В противном случае к 'code' добавляется только название гейтвея.
        """

        signals = signals.copy()

        # Вычисление количества записей для каждой комбинации 'address' и 'common_address':
        group_counts = signals.groupby([settings.ADDRESS_COLUMN, settings.COMMON_ADDRESS_COLUMN]).transform('size')

        # Изменение столбца code:
        signals[settings.CODE_COLUMN] = np.where(
            group_counts > 1,
            group_counts.astype(str) + '_signals_' +
            signals[settings.GATEWAY_COLUMN] + '_' +
            signals[settings.ADDRESS_COLUMN],
            signals[settings.CODE_COLUMN] + '_' + signals[settings.GATEWAY_COLUMN]
        )

        # Удаление дубликатов:
        signals = signals.drop_duplicates(subset=[settings.CODE_COLUMN])
        return signals

    @staticmethod
    def concatenate_devices(signals: pd.DataFrame) -> pd.DataFrame:
        """
        Объединяет (конкатенирует) название датчиков, если они имеют одинаковый slave_id (common address)

        Параметры:
        - signals: DataFrame, в котором каждый датчик представлен в единственном экзепляре

        Возвращает:
        - DataFrame с обновленным столбцом 'device', содержащим названия устройств,
        перечисленные через запятую, если они имеют одинаковый slave_id.
        """

        # Создание копии DataFrame для избежания предупреждений о присвоении
        signals = signals.copy()

        # Конкатенация названий устройств
        signals[settings.SIGNALS_SHEET_DEVICE_COLUMN] = signals.groupby(
            settings.COMMON_ADDRESS_COLUMN
        )[settings.SIGNALS_SHEET_DEVICE_COLUMN].transform(
            lambda x: ', '.join(x.unique())
        )
        return signals

    @staticmethod
    def fill_missing_data_types(signals: pd.DataFrame) -> pd.DataFrame:
        """
        Добавляет типы данных в DataFrame сигналов, устанавливая отсутствующие типы данных в 'hfloat'.

        Параметры:
        - signals: DataFrame, содержащий сигналы, возможно с отсутствующими значениями в столбце типа данных.

        Возвращает:
        - DataFrame с обновленным столбцом типа данных, в котором отсутствующие значения заменены на 'hfloat'.
        """

        missing_count = signals[settings.VALUE_TYPE_COLUMN].isnull().sum()
        if missing_count > 0:
            logging.warning(f"В столбце {settings.VALUE_TYPE_COLUMN} отсутствуют значения в {missing_count} строчках")
            # Заполнение отсутствующих значений:
            signals.loc[:, settings.VALUE_TYPE_COLUMN] = signals[settings.VALUE_TYPE_COLUMN].fillna('hfloat')
            logging.info(f"{missing_count} сигналам установлен тип hfloat")
        else:
            logging.info(f"Отсутствующие значения в столбце {settings.VALUE_TYPE_COLUMN} не обнаружены.")
        return signals

    @staticmethod
    def divide_by_assets(signals: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """
        Разбивает сигналы по ассетам (если переменная DIVIDE_CONFIG_BY_ASSET или DIVIDE_DATA_BY_ASSET
        в .env файле в состоянии True).

        Параметры:
        - signals: DataFrame, содержащий все сигналы.

        Возвращает:
        - dict: словарь из ключей с названием ассетов и значений (с Dataframe сигналов этих ассетов),
         ключ "all_assets" - и общий Dataframe сигналов если разделение не требуется.
        """

        signals_divided_by_assets = {'all_assets': signals}

        if settings.DIVIDE_CONFIG_BY_ASSET or settings.DIVIDE_DATA_BY_ASSET:
            assets = signals[settings.ASSET_COLUMN].unique()
            for asset in assets:
                signals_by_asset = signals[signals[settings.ASSET_COLUMN] == asset]
                signals_divided_by_assets[asset] = signals_by_asset
        return signals_divided_by_assets


class DataMapper:
    """Класс для создания маппингов."""

    @staticmethod
    def create_data_mapping(asset: str, signals: pd.DataFrame) -> dict:
        """
        Создает словарь для конфига взаимосвязи между кодами в excel файле и эмуляторе.
        В данной реализации коды сигналов в excel файле соответствуют именам в эмуляторе.

        Параметры:
        - asset: оборудование для которого создается маппинг
        - signals: DataFrame, содержащий информацию о сигналах, включая столбцы с кодами и типами данных.

        Возвращает:
        - dict: Словарь, где ключами являются коды сигналов, а значениями — словари с ключами:
            - "type": тип данных сигнала.
            - "base": список, содержащий имя файла данных и код сигнала.

        Пример возвращаемого значения:
        {
            'code1': {
                'type': 'hfloat',
                'base': ['data.xlsx', 'code1']
            },
            'code2': {
                'type': 'hint',
                'base': ['data.xlsx', 'code2']
            },
            ...
        }
        """

        mapping = {}
        for _, row in signals.iterrows():
            code = row[settings.CODE_COLUMN]
            if settings.DIVIDE_DATA_BY_ASSET:
                file_suffix = row[settings.ASSET_COLUMN]
            else:
                file_suffix = "all_assets"
            mapping[code] = {
                "type": row[settings.VALUE_TYPE_COLUMN],
                "base": [f"{settings.EXCEL_DATA_NAME}_{file_suffix}.xlsx", code]
            }
        return mapping

    @staticmethod
    def create_slaves_mapping(signals: pd.DataFrame) -> dict:
        """
        Создает словарь для маппинга в конфиге devices(датчиков) на их slave_id и содержащиеся в них регистры.

        Параметры:
        - signals (pd.DataFrame): DataFrame, содержащий информацию о сигналах, включая столбцы устройств,
        общих адресов, адресов и кодов сигналов.

        Возвращает:
        - dict: Словарь, где ключами являются названия устройств, а значениями — словари с ключами:
            - "slaveID": целое число, общее для устройства.
            - "holdings": словарь, маппящий адреса раегистров на коды сигналов.

        Пример возвращаемого значения:
        {
            'device1': {
                'slaveID': 1,
                'holdings': {100: 'code1', 101: 'code2'}
            },
            'device2': {
                'slaveID': 2,
                'holdings': {200: 'code3', 201: 'code4'}
            },
            ...
        }
        """

        mapping = {}
        uniqe_device = signals[settings.SIGNALS_SHEET_DEVICE_COLUMN].unique()
        for device in uniqe_device:
            device_signals = signals.loc[signals[settings.SIGNALS_SHEET_DEVICE_COLUMN] == device]
            mapping[device] = {
                "slaveID": int(device_signals[settings.COMMON_ADDRESS_COLUMN].iloc[0]),
                "holdings": device_signals.set_index(settings.ADDRESS_COLUMN)[settings.CODE_COLUMN].to_dict()
            }
        return mapping

    @staticmethod
    def create_signals_template(signals: pd.DataFrame) -> pd.DataFrame:
        """
        Создает пустой DataFrame с колонками, соответствующими кодам сигналов из входного DataFrame.

        Параметры:
        - signals: DataFrame, содержащий столбец с кодами сигналов.

        Возвращает:
        - pd.DataFrame: Пустой DataFrame, колонки которого — коды сигналов.
        """

        # Проверяем, содержит ли DataFrame необходимый столбец
        if settings.CODE_COLUMN not in signals.columns:
            raise ValueError(f"Входной DataFrame не содержит столбца '{settings.CODE_COLUMN}'.")

        return pd.DataFrame(columns=signals[settings.CODE_COLUMN])


class ConfigGenerator:
    """Класс для генерации конифга эмулятора."""

    @staticmethod
    def generate_config(data_mapping: dict, emulator_mapping: dict) -> dict:
        """
            Генерирует итоговый конфигурационный словарь для эмулятора.

            Параметры:
            data_mapping: dict, словарь с маппингом между кодами в excel файле и эмуляторе.
            emulator_mapping: dict, словарь с маппингом devices(датчиков) на их slave_id и регистры.

            Возвращает:
            - dict: Конфигурационный словарь, готовый для использования эмулятором.
            """
        config = {
            "signals": data_mapping,
            "servers": {
                "Test": {
                    "host": settings.HOST,
                    "port": settings.PORT,
                    "period": [settings.HOURS, settings.MINUTES, settings.SECONDS],
                    "slaves": emulator_mapping
                }
            }
        }
        return config


class FileCreator:
    """Класс для создания конфигурационных файлов."""

    @staticmethod
    def create_json_with_config(config: dict, asset: str) -> NoReturn:
        """
        Сохраняет конфигурационный словарь в JSON-файл.

        Параметры:
        - config: dict, конфигурационный словарь для сохранения.

        Возвращает:
        - None
        """
        if not settings.DIVIDE_CONFIG_BY_ASSET and asset == 'all_assets':
            with open(f"{settings.JSON_CONFIG_FILE}_{asset}.json", "w", encoding="utf-8") as json_file:
                json.dump(config, json_file, ensure_ascii=False, indent=4)
                logging.info(f"Файл {settings.JSON_CONFIG_FILE}_{asset}.json успешно создан")
        elif settings.DIVIDE_CONFIG_BY_ASSET and asset != 'all_assets':
            with open(f"{settings.JSON_CONFIG_FILE}_{asset}.json", "w", encoding="utf-8") as json_file:
                json.dump(config, json_file, ensure_ascii=False, indent=4)
                logging.info(f"Файл {settings.JSON_CONFIG_FILE}_{asset}.json успешно создан")


    @staticmethod
    def create_excel_data_template(data_mapping: pd.DataFrame, asset: str) -> NoReturn:
        """
        Сохраняет коды сигналов из DataFrame в Excel-файл.

        Параметры:
        - data_mapping: DataFrame, содержащий данные для сохранения в Excel.

        Возвращает:
        - None
        """
        if not settings.DIVIDE_DATA_BY_ASSET and asset == 'all_assets':
            data_mapping.to_excel(f"{settings.EXCEL_DATA_FILE}_{asset}.xlsx")
            logging.info(f"Файл {settings.EXCEL_DATA_FILE}_{asset}.xlsx успешно создан")
        elif settings.DIVIDE_DATA_BY_ASSET and asset != 'all_assets':
            data_mapping.to_excel(f"{settings.EXCEL_DATA_FILE}_{asset}.xlsx")
            logging.info(f"Файл {settings.EXCEL_DATA_FILE}_{asset}.xlsx успешно создан")

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
        file_creator = FileCreator()
        file_creator.create_json_with_config(config, asset)
        file_creator.create_excel_data_template(signals_template, asset)
    logging.info(f"Все файлы созданы")


if __name__ == "__main__":
    main()
