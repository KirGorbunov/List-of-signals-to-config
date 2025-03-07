import json
import pandas as pd
from typing import NoReturn
from src.settings import settings
import logging


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