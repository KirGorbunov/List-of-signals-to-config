import pandas as pd
from settings import settings

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
