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
    def get_measured_rows(df):
        measured_signals = df.loc[df[settings.SIGNAL_TYPE_COLUMN] == settings.ONLY_SIGNALS_TYPE]
        measured_signals_with_address = measured_signals[measured_signals[settings.ADDRESS_COLUMN].notna()]
        return measured_signals_with_address

    def change_code_name(signals):
        signals['count'] = signals.groupby(['address', 'common_address'])['address'].transform('count')
        signals['count'] = signals['count'].fillna(0)
        signals['count'] = signals['count'].astype(int)
        signals['code'] = np.where(signals['count'] > 1,
                                   signals['count'].astype(str) + '_signals_' + signals['gateway'] + '_' + signals[
                                       'address'],
                                   signals['code'] + '_' + signals['gateway'])
        signals = signals.drop('count', axis=1)
        return signals

    def get_measured_signals(signals):
        uniq_signals = signals.drop_duplicates(subset=[settings.CODE_COLUMN], keep='last')
        return uniq_signals


class DataMappingConfigurator(Configurator):
    def get_data_mapping(signals):
        mapping = {}
        for _, row in signals.iterrows():
            code = row[settings.CODE_COLUMN]
            mapping[code] = {
                "type": row[settings.VALUE_TYPE_COLUMN],
                "base": [settings.EXCEL_DATA_FILE, code]
            }
        return mapping


class EmulatorConfigurator(Configurator):
    def get_slaves_mapping(signals):
        mapping = {}
        uniqe_gateway = signals[settings.GATEWAY_COLUMN].unique()
        for gateway in uniqe_gateway:
            device_signals = signals.loc[signals[settings.GATEWAY_COLUMN] == gateway]
            mapping[gateway] = {
                "slaveID": device_signals[settings.COMMON_ADDRESS_COLUMN].iloc[0],
                "holdings": device_signals.set_index(settings.ADDRESS_COLUMN)[settings.CODE_COLUMN].to_dict()
            }
        return mapping

    def get_config(signals_for_mapping, emulator_mapping):
        config = {
            "signals": signals_for_mapping,
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
    def get_data_mapping(signals):
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
    unique_signals = Configurator.get_measured_signals(signals)
    logging.debug("Dataframe prepared")

    # Creating mappings:
    signals_for_mapping = DataMappingConfigurator.get_data_mapping(unique_signals)
    emulator_mapping = EmulatorConfigurator.get_slaves_mapping(unique_signals)
    data_mapping = DataConfigurator.get_data_mapping(unique_signals)

    config = EmulatorConfigurator.get_config(signals_for_mapping, emulator_mapping)
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
