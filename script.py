import json
import pandas as pd
import logging

from settings import settings


logging.basicConfig(
    level=settings.LOGGING_LEVEL,
    format="{levelname} - {message}",
    style="{",
)

def preparing_data(excel_signals: pd.DataFrame, excel_devices: pd.DataFrame) -> pd.DataFrame:
    '''Подготовка и преобразование двух датасетов к одному'''

    excel_devices = excel_devices.rename(
        columns={settings.DEVICES_SHEET_DEVICE_COLUMN: settings.SIGNALS_SHEET_DEVICE_COLUMN}
)
    general = pd.merge(excel_signals,
                       excel_devices,
                       on=settings.SIGNALS_SHEET_DEVICE_COLUMN,
                       how="left")
    signals = general.loc[general[settings.SIGNALS_SHEET_SIGNAL_TYPE_COLUMN] == settings.ONLY_SIGNALS_TYPE]
    return signals

def dataframe_to_dicts(signals: pd.DataFrame) -> (dict):
    '''Создание из датасета словаря необходимой для эмулятора конфигурации'''

    uniqe_gateway = signals[settings.DEVICES_SHEET_GATEWAY_COLUMN].unique()
    signals_for_excel = {}
    slaves = {}

    for gateway in uniqe_gateway:
        device_signals = signals.loc[signals[settings.DEVICES_SHEET_GATEWAY_COLUMN] == gateway]
        uniq_addresses = get_uniq_addresses(device_signals)
        not_uniq_addresses = get_not_uniq_addresses(device_signals, gateway)
        union_addresses = uniq_addresses | not_uniq_addresses
        signals_for_excel = get_signals_for_excel(union_addresses, signals_for_excel)
        slaves = get_slaves(gateway, device_signals, union_addresses, slaves)

    config = {
        "signals": signals_for_excel,
        "servers": {
            "Test": {
                "host": settings.HOST,
                "port": settings.PORT,
                "period": [settings.HOURS, settings.MINUTS, settings.SECONDS],
                "slaves": slaves
            }
        }
    }

    return signals_for_excel, config

def get_uniq_addresses(signals: pd.DataFrame) -> dict:
    '''Создание словаря с уникальными сигналами'''

    uniq_signals = signals.drop_duplicates(subset=[settings.SIGNALS_SHEET_ADDRESS_COLUMN], keep=False)
    uniq_signals.loc[:, settings.SIGNALS_SHEET_CODE_COLUMN] = (
            uniq_signals[settings.SIGNALS_SHEET_CODE_COLUMN] + "_" + uniq_signals[settings.DEVICES_SHEET_GATEWAY_COLUMN]
    )
    uniq_address = uniq_signals.set_index(
        settings.SIGNALS_SHEET_ADDRESS_COLUMN
    )[
        settings.SIGNALS_SHEET_CODE_COLUMN
    ].to_dict()
    return uniq_address

def get_not_uniq_addresses(signals: pd.DataFrame, gateway: str) -> dict:
    '''Создание словаря с неуникальными сигналами,
     т.е. когда на одном регистре хранится несколько сигналов в разных битах'''

    complex_signals = {}
    signals_addr_not_uniq = signals[signals.duplicated(subset=[settings.SIGNALS_SHEET_ADDRESS_COLUMN], keep=False)]
    for address in signals_addr_not_uniq[settings.SIGNALS_SHEET_ADDRESS_COLUMN].unique():
        signals_with_same_address = signals_addr_not_uniq.loc[
            signals_addr_not_uniq[settings.SIGNALS_SHEET_ADDRESS_COLUMN] == address
        ]
        if not signals_with_same_address.empty:
            complex_signals[address] = f"{len(signals_with_same_address)}_signals_{gateway}_{address}"
    return complex_signals

def get_signals_for_excel(union_addresses: dict, signals_for_excel: dict) -> dict:
    '''Создание словаря конфигуратора используемого для маппинга с excel-файлом c данными'''

    for code in union_addresses.values():
        signals_for_excel[code] = {
            "type": "ushort-float32",
            "base": [settings.EXCEL_DATA_FILE, code]
        }
    return signals_for_excel

def get_slaves(gateway: str, device_signals: dict, union_addresses: dict, slaves: dict) -> dict:
    '''Создание словаря со всеми сигналами разбитыми по слейвам'''

    if union_addresses:
        slaves[gateway] = {
            "slaveID": int(device_signals.iloc[0][settings.DEVICES_SHEET_COMMON_ADDRESS_COLUMN]),
            "holdings": union_addresses
        }
    return slaves

def creating_files(signals_for_excel: dict, config: dict):
    '''Создание файлов'''

    all_codes = pd.DataFrame(columns=signals_for_excel.keys())
    all_codes.to_excel(settings.EXCEL_DATA_FILE)

    with open(settings.JSON_CONFIG_FILE, "w", encoding="utf-8") as json_file:
        json.dump(config, json_file, ensure_ascii=False)

def excel_to_json(excel_signals: pd.DataFrame, excel_devices: pd.DataFrame):
    signals = preparing_data(excel_signals, excel_devices)
    logging.debug("Data prepared")

    signals_for_excel, config = dataframe_to_dicts(signals)
    logging.debug("Config have been created successfully")

    creating_files(signals_for_excel, config)
    logging.info(f"Files {settings.EXCEL_DATA_FILE} and {settings.JSON_CONFIG_FILE} "
                 f"have been created successfully")


if __name__ == "__main__":
    excel_signals = pd.read_excel(settings.LIST_OF_SIGNALS_FILE,
                                  sheet_name=settings.SIGNALS_SHEET,
                                  usecols=[settings.SIGNALS_SHEET_DEVICE_COLUMN,
                                           settings.SIGNALS_SHEET_CODE_COLUMN,
                                           settings.SIGNALS_SHEET_SIGNAL_TYPE_COLUMN,
                                           settings.SIGNALS_SHEET_ADDRESS_COLUMN],
                                  dtype="string")
    excel_devices = pd.read_excel(settings.LIST_OF_SIGNALS_FILE,
                                  settings.DEVICES_SHEET,
                                  usecols=[settings.DEVICES_SHEET_GATEWAY_COLUMN,
                                           settings.DEVICES_SHEET_DEVICE_COLUMN,
                                           settings.DEVICES_SHEET_COMMON_ADDRESS_COLUMN],
                                  dtype="string")
    logging.info(f"Script starting with sheets signals (len={excel_signals.shape[0]}) "
                 f"and devices (len={excel_devices.shape[0]})")
    excel_to_json(excel_signals, excel_devices)
