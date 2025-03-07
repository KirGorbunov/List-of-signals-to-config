import pandas as pd
from src.settings import settings


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