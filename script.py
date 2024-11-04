import json
import logging

import numpy as np
import pandas as pd

from settings import settings

logging.basicConfig(
    level=settings.LOGGING_LEVEL,
    format="{levelname} - {message}",
    style="{",
)


class DatasetConstractor:
    def merge_signals_and_devices(signal_data: pd.DataFrame, device_data: pd.DataFrame) -> pd.DataFrame:
        """
        Объединяет два датасета signals и devices по общему столбцу device.

        Параметры:
        - signal_data: DataFrame с данными сигналов.
        - device_data: DataFrame с данными устройств.

        Возвращает:
        - DataFrame, содержащий объединенные данные сигналов и устройств.
        """

        # Переименование столбца в датасете devices для соответствия имени столбца в датасете signals
        renamed_device_data = device_data.rename(
            columns={settings.DEVICES_SHEET_DEVICE_COLUMN: settings.SIGNALS_SHEET_DEVICE_COLUMN}
        )

        # Объединение датасетов по общему столбцу device
        merged_data = pd.merge(signal_data,
                               renamed_device_data,
                               on=settings.SIGNALS_SHEET_DEVICE_COLUMN,
                               how="left")
        return merged_data


class Configurator:
    def get_measured_rows(df: pd.DataFrame) -> pd.DataFrame:
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

    def change_code_name(signals: pd.DataFrame) -> pd.DataFrame:
        """
        Меняет столбец 'code', если количество записей для каждой комбинации
        'address' и 'common_address' > 1, т.е. когда на одном регистре содержиться несколько сигналов.

        Параметры:
        - signals: общий DataFrame cо всеми измеряемыми сигналами

        Возвращает:
        - исходный DataFrame с измененным столбцом 'code'.
        Если для комбинации ('address', 'common_address') существует несколько сигналов, к 'code' добавляется
        количество сигналов, название гейтвея и modbus-адрес. В противном случае к 'code' добавляется только
        название гейтвея.
        """

        # Вычисление количества записей для каждой комбинации 'address' и 'common_address':
        signal_counts = signals.groupby(['address', 'common_address']).transform('size')

        # Изменение столбца code:
        signals['code'] = np.where(
            signal_counts > 1,
            signal_counts.astype(str) + '_signals_' + signals['gateway'] + '_' + signals['address'],
            signals['code'] + '_' + signals['gateway']
        )
        return signals

    def get_unique_signals(signals: pd.DataFrame) -> pd.DataFrame:
        """
        Удаляет из DataFrame повторные составные сигналы, оставляя только один экзепляр.

        Параметры:
        - signals: DataFrame c дублированием кодов состовных сигналов

        Возвращает:
        - DataFrame с уникальными сигналами, где для каждого кода сохранена одна запись.
        """

        return signals.drop_duplicates(subset=[settings.CODE_COLUMN]).copy()

    def concatenate_devices(signals: pd.DataFrame) -> pd.DataFrame:
        """
        Объединяет (конкатенирует) название датчиков, если они имеют slave_id (common address)

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

    def add_data_type(signals: pd.DataFrame) -> pd.DataFrame:
        """
            Добавляет типы данных в DataFrame сигналов, устанавливая отсутствующие типы данных в 'hfloat'.

            Параметры:
            - signals: DataFrame, содержащий сигналы, возможно с отсутствующими значениями в столбце типа данных.

            Возвращает:
            - DataFrame с обновленным столбцом типа данных, в котором отсутствующие значения заменены на 'hfloat'.
        """

        missing_count = signals[settings.VALUE_TYPE_COLUMN].isnull().sum()
        if missing_count > 0:
            logging.warning(f"В столбце {settings.SIGNAL_TYPE_COLUMN} отсутствуют значения в {missing_count} строчках")
            # Заполнение отсутствующих значений:
            signals.loc[:, settings.VALUE_TYPE_COLUMN] = signals[settings.VALUE_TYPE_COLUMN].fillna('hfloat')
            logging.info(f"{missing_count} сигналам установлен тип hfloat")
        else:
            logging.info("Отсутствующие значения в столбце {settings.SIGNAL_TYPE_COLUMN} не обнаружены.")
        return signals


class DataMappingConfigurator(Configurator):
    def get_data_mapping(signals: pd.DataFrame) -> dict:
        """
            Создает словарь для конфига взаимосвязи между кодами в excel файле и эмуляторе.
            В данной реализации коды сигналов в excel файле соответствуют именам в эмуляторе.

            Параметры:
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
            mapping[code] = {
                "type": row[settings.VALUE_TYPE_COLUMN],
                "base": [settings.EXCEL_DATA_FILE, code]
            }
        return mapping


class EmulatorConfigurator(Configurator):
    def get_slaves_mapping(signals: pd.DataFrame) -> dict:
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

    def get_config(data_mapping: dict, emulator_mapping: dict) -> dict:
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
                    "period": [settings.HOURS, settings.MINUTS, settings.SECONDS],
                    "slaves": emulator_mapping
                }
            }
        }
        return config


class DataConfigurator(Configurator):
    def get_signals_code(signals: pd.DataFrame) -> pd.DataFrame:
        """
        Создает пустой DataFrame с колонками, соответствующими кодам сигналов из входного DataFrame.

        Параметры:
        - signals: DataFrame, содержащий столбец с кодами сигналов (settings.CODE_COLUMN).

        Возвращает:
        - pd.DataFrame: Пустой DataFrame, колонки которого — уникальные коды сигналов из столбца settings.CODE_COLUMN.
        """
        # Проверяем, содержит ли DataFrame необходимый столбец
        if settings.CODE_COLUMN not in signals.columns:
            raise ValueError(f"Входной DataFrame не содержит столбца '{settings.CODE_COLUMN}'.")

        return pd.DataFrame(columns=signals[settings.CODE_COLUMN])


class FileCreator:
    pass


class ConfigCreator(FileCreator):
    def json_creator(config):
        with open(settings.JSON_CONFIG_FILE, "w", encoding="utf-8") as json_file:
            json.dump(config, json_file, ensure_ascii=False)


class DataTemplateCreator(FileCreator):
    def data_excel_template_creator(data_mapping):
        data_mapping.to_excel(settings.EXCEL_DATA_FILE)


def excel_to_json(excel_signals: pd.DataFrame, excel_devices: pd.DataFrame):
    # Creating dataframe:
    general = DatasetConstractor.merge_signals_and_devices(excel_signals, excel_devices)
    logging.debug("Dataframe created")

    # Dataframe prepare:
    signals = Configurator.get_measured_rows(general)
    signals = Configurator.change_code_name(signals)
    signals = Configurator.concatenate_devices(signals)
    unique_signals = Configurator.get_unique_signals(signals)
    normalize_signals = Configurator.add_data_type(unique_signals)
    logging.debug("Dataframe prepared")

    # Creating mappings:
    emulator_mapping = EmulatorConfigurator.get_slaves_mapping(normalize_signals)
    data_mapping = DataConfigurator.get_signals_code(normalize_signals)

    config = EmulatorConfigurator.get_config(data_mapping, emulator_mapping)
    logging.debug("Mapping prepared")

    # Creating files:
    ConfigCreator.json_creator(config)
    DataTemplateCreator.data_excel_template_creator(data_mapping)
    logging.info(f"Files {settings.EXCEL_DATA_FILE} and {settings.JSON_CONFIG_FILE} "
                 f"have been created successfully")


if __name__ == "__main__":
    excel_signals = pd.read_excel(settings.LIST_OF_SIGNALS_FILE,
                                  sheet_name=settings.SIGNALS_SHEET,
                                  usecols=[settings.SIGNALS_SHEET_DEVICE_COLUMN,
                                           settings.CODE_COLUMN,
                                           settings.SIGNAL_TYPE_COLUMN,
                                           settings.ADDRESS_COLUMN,
                                           settings.VALUE_TYPE_COLUMN],
                                  dtype="string")
    excel_devices = pd.read_excel(settings.LIST_OF_SIGNALS_FILE,
                                  settings.DEVICES_SHEET,
                                  usecols=[settings.GATEWAY_COLUMN,
                                           settings.DEVICES_SHEET_DEVICE_COLUMN,
                                           settings.COMMON_ADDRESS_COLUMN],
                                  dtype="string")
    logging.info(f"Script starting with sheets signals (len={excel_signals.shape[0]}) "
                 f"and devices (len={excel_devices.shape[0]})")
    excel_to_json(excel_signals, excel_devices)
