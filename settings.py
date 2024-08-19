from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Названия файлов:
    LIST_OF_SIGNALS_FILE: str
    EXCEL_DATA_FILE: str = "data.xlsx"
    JSON_CONFIG_FILE: str = "config.json"

    # Названия страниц в excel файле:
    SIGNALS_SHEET: str = "signals"
    DEVICES_SHEET: str = "devices"

    # Названия столбцов:
    SIGNALS_SHEET_DEVICE_COLUMN: str = "device"
    SIGNALS_SHEET_CODE_COLUMN: str = "code"
    SIGNALS_SHEET_SIGNAL_TYPE_COLUMN: str = "signal_type"
    SIGNALS_SHEET_ADDRESS_COLUMN: str = "address"

    DEVICES_SHEET_GATEWAY_COLUMN: str = "gateway"
    DEVICES_SHEET_DEVICE_COLUMN: str = "code"
    DEVICES_SHEET_COMMON_ADDRESS_COLUMN: str = "common_address"

    # Необходимый тип сигнала:
    ONLY_SIGNALS_TYPE: str = "Сигнал"

    # IP-адрес и порт:
    HOST: str = "127.0.0.1"
    PORT: int = 502

    # Конфигурирование таймера эмулятора:
    HOURS: int = 0
    MINUTS: int = 10
    SECONDS: int = 0

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
