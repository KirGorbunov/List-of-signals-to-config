import json
import pandas as pd

from settings import settings


def get_uniq_addresses(signals: pd.DataFrame) -> dict:
    uniq_signals = signals.drop_duplicates(subset=[settings.SIGNALS_SHEET_ADDRESS_COLUMN_NAME], keep=False)
    uniq_signals.loc[:, settings.SIGNALS_SHEET_CODE_COLUMN_NAME] = uniq_signals[settings.SIGNALS_SHEET_CODE_COLUMN_NAME] + '_' + uniq_signals[settings.DEVICES_SHEET_GATEWAY_COLUMN_NAME]
    uniq_address = uniq_signals.set_index(settings.SIGNALS_SHEET_ADDRESS_COLUMN_NAME)[settings.SIGNALS_SHEET_CODE_COLUMN_NAME].to_dict()
    return uniq_address


def get_not_uniq_addresses(signals: pd.DataFrame, gateway: str) -> dict:
    complex_signals = {}
    signals_addr_not_uniq = signals[signals.duplicated(subset=[settings.SIGNALS_SHEET_ADDRESS_COLUMN_NAME], keep=False)]
    for address in signals_addr_not_uniq[settings.SIGNALS_SHEET_ADDRESS_COLUMN_NAME].unique():
        signals_with_same_address = signals_addr_not_uniq.loc[signals_addr_not_uniq[settings.SIGNALS_SHEET_ADDRESS_COLUMN_NAME] == address]
        if not signals_with_same_address.empty:
            complex_signals[address] = f'{len(signals_with_same_address)}_signals_{gateway}_{address}'
    return complex_signals


def excel_to_json(excel_signals: pd.DataFrame, excel_devices: pd.DataFrame):
    excel_devices = excel_devices.rename(
        columns={settings.DEVICES_SHEET_DEVICE_COLUMN_NAME: settings.SIGNALS_SHEET_DEVICE_COLUMN_NAME}
    )
    general = pd.merge(excel_signals,
                       excel_devices,
                       on=settings.SIGNALS_SHEET_DEVICE_COLUMN_NAME,
                       how='left')
    signals = general.loc[general[settings.SIGNALS_SHEET_SIGNAL_TYPE_COLUMN_NAME] == settings.ONLY_SIGNALS_TYPE]
    uniqe_gateway = signals[settings.DEVICES_SHEET_GATEWAY_COLUMN_NAME].unique()
    slaves = {}
    signals_for_excel = {}

    for gateway in uniqe_gateway:
        device_signals = signals.loc[signals[settings.DEVICES_SHEET_GATEWAY_COLUMN_NAME] == gateway]
        uniq_addresses = get_uniq_addresses(device_signals)
        not_uniq_addresses = get_not_uniq_addresses(device_signals, gateway)
        union_addresses = uniq_addresses | not_uniq_addresses
        for code in union_addresses.values():
            signals_for_excel[code] = {
                "type": "ushort-float32",
                "base": [settings.EXCEL_DATA_FILE_NAME, code]
            }

        if union_addresses:
            slaves[gateway] = {
                "slaveID": int(device_signals.iloc[0][settings.DEVICES_SHEET_COMMON_ADDRESS_COLUMN_NAME]),
                "holdings": union_addresses
            }

    all_codes = pd.DataFrame(columns=signals_for_excel.keys())
    all_codes.to_excel(settings.EXCEL_DATA_FILE_NAME)

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

    with open(settings.JSON_CONFIG_FILE_NAME, 'w', encoding='utf-8') as json_file:
        json.dump(config, json_file, ensure_ascii=False)


if __name__ == "__main__":
    excel_signals = pd.read_excel(settings.LIST_OF_SIGNALS_FILE_NAME,
                                  sheet_name=settings.SIGNALS_SHEET_NAME,
                                  usecols=[settings.SIGNALS_SHEET_DEVICE_COLUMN_NAME,
                                           settings.SIGNALS_SHEET_CODE_COLUMN_NAME,
                                           settings.SIGNALS_SHEET_SIGNAL_TYPE_COLUMN_NAME,
                                           settings.SIGNALS_SHEET_ADDRESS_COLUMN_NAME],
                                  dtype='string')
    excel_devices = pd.read_excel(settings.LIST_OF_SIGNALS_FILE_NAME,
                                  settings.DEVICES_SHEET_NAME,
                                  usecols=[settings.DEVICES_SHEET_GATEWAY_COLUMN_NAME,
                                           settings.DEVICES_SHEET_DEVICE_COLUMN_NAME,
                                           settings.DEVICES_SHEET_COMMON_ADDRESS_COLUMN_NAME],
                                  dtype='string')
    excel_to_json(excel_signals, excel_devices)
