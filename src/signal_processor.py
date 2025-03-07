import numpy as np
import pandas as pd
from src.settings import settings
import logging


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
