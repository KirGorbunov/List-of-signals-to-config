import json
import pandas as pd
from typing import NoReturn
from src.settings import settings
import logging


class FileCreator:
    """Класс для создания конфигурационных файлов."""

    def __init__(self, asset: str, data_mapping: pd.DataFrame, config: dict):
        """
        file_type: Тип файла - "json" или "excel".
        """
        self.asset = asset
        self.data_mapping = data_mapping
        self.config = config
        self.json_file_name = settings.JSON_CONFIG_FILE.with_name(f"{settings.JSON_CONFIG_NAME}_{self.asset}.json")
        self.excel_file_name = settings.EXCEL_DATA_FILE.with_name(f"{settings.EXCEL_DATA_NAME}_{self.asset}.xlsx")


    def create_folder(func):
        """Декоратор для создания папок перед сохранением файлов."""
        def wrapper(self, *args, **kwargs):
            self.json_file_name.parent.mkdir(exist_ok=True, parents=True)
            self.excel_file_name.parent.mkdir(exist_ok=True, parents=True)
            return func(self, *args, **kwargs)
        return wrapper


    @create_folder
    def create_json_with_config(self) -> NoReturn:
        """
        Сохраняет конфигурационный словарь в JSON-файл.

        Параметры:
        - config: dict, конфигурационный словарь для сохранения.

        Возвращает:
        - None
        """
        if ((not settings.DIVIDE_CONFIG_BY_ASSET and self.asset == 'all_assets') or
                (settings.DIVIDE_CONFIG_BY_ASSET and self.asset != 'all_assets')):
            with open(self.json_file_name, "w", encoding="utf-8") as json_file:
                json.dump(self.config, json_file, ensure_ascii=False, indent=4)
                logging.info(f"Файл {self.json_file_name} успешно создан")


    @create_folder
    def create_excel_data_template(self) -> NoReturn:
        """
        Сохраняет коды сигналов из DataFrame в Excel-файл.

        Параметры:
        - data_mapping: DataFrame, содержащий данные для сохранения в Excel.

        Возвращает:
        - None
        """
        if ((not settings.DIVIDE_DATA_BY_ASSET and self.asset == "all_assets") or
                (settings.DIVIDE_DATA_BY_ASSET and self.asset != "all_assets")):
            self.data_mapping.to_excel(self.excel_file_name)
            logging.info(f"Файл {self.excel_file_name} успешно создан")
