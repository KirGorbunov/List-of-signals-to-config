from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Логика деления на файлы:
    DIVIDE_CONFIG_BY_ASSET: bool = True
    DIVIDE_DATA_BY_ASSET: bool = True

    # Названия файлов:
    LIST_OF_SIGNALS_FILE: str
    EXCEL_DATA_FILE: str = "data"
    JSON_CONFIG_FILE: str = "config"

    # Названия страниц в excel файле:
    SIGNALS_SHEET: str = "signals"
    DEVICES_SHEET: str = "devices"

    # Названия столбцов:
    SIGNALS_SHEET_DEVICE_COLUMN: str = "device"
    CODE_COLUMN: str = "code"
    SIGNAL_TYPE_COLUMN: str = "signal_type"
    ADDRESS_COLUMN: str = "address"
    VALUE_TYPE_COLUMN: str = "value_type"
    ASSET_COLUMN: str = "asset"

    GATEWAY_COLUMN: str = "gateway"
    DEVICES_SHEET_DEVICE_COLUMN: str = "code"
    COMMON_ADDRESS_COLUMN: str = "common_address"

    # Необходимый тип сигнала:
    ONLY_SIGNALS_TYPE: str = "Сигнал"

    # IP-адрес и порт:
    HOST: str = "0.0.0.0"
    PORT: int = 502

    # Конфигурирование таймера эмулятора:
    HOURS: int = 0
    MINUTES: int = 10
    SECONDS: int = 0

    # Уровень логгирования:
    LOGGING_LEVEL: str = 'INFO'

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
