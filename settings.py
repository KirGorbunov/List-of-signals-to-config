from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    LIST_OF_SIGNALS_FILE_NAME: str

    SIGNALS_SHEET_NAME: str = "signals"
    SIGNALS_SHEET_DEVICE_COLUMN_NAME: str = "device"
    SIGNALS_SHEET_CODE_COLUMN_NAME: str = "code"
    SIGNALS_SHEET_SIGNAL_TYPE_COLUMN_NAME: str = "signal_type"
    SIGNALS_SHEET_ADDRESS_COLUMN_NAME: str = "address"
    ONLY_SIGNALS_TYPE: str = "Сигнал"

    DEVICES_SHEET_NAME: str = "devices"
    DEVICES_SHEET_GATEWAY_COLUMN_NAME: str = "gateway"
    DEVICES_SHEET_DEVICE_COLUMN_NAME: str = "code"
    DEVICES_SHEET_COMMON_ADDRESS_COLUMN_NAME: str = "common_address"

    EXCEL_DATA_FILE_NAME: str = "data.xlsx"

    JSON_CONFIG_FILE_NAME: str = "config.json"
    HOST: str = "127.0.0.1"
    PORT: int = 502
    HOURS: int = 0
    MINUTS: int = 10
    SECONDS: int = 0

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()











#     POSTGRES_DB: str
#     POSTGRES_USER: str
#     POSTGRES_PASSWORD: str
#     POSTGRES_HOST: str = "127.0.0.1"
#     POSTGRES_PORT: str = "5432"
#     ELASTIC_PORT: str = "9200"
#     ELASTIC_HOST: str = "localhost"
#
#     PERSON_LIMIT: str = "50"
#     GENRE_LIMIT: str = "1"
#     FILMWORK_LIMIT: str = "200"
#     RELAX_TIME: int = 2
#
#     TIMER_GENRES: str = "2020-06-16T20:14:09.310000+00:00"
#     TIMER_PERSONS: str = "2020-06-16T20:14:09.310000+00:00"
#     TIMER_FILMWORKS: str = "2020-06-16T20:14:09.310000+00:00"
#
#     model_config = SettingsConfigDict(env_file="../.env", env_file_encoding="utf-8")
#
#     @property
#     def dsl(self) -> dict:
#         return {
#             "dbname": self.POSTGRES_DB,
#             "user": self.POSTGRES_USER,
#             "password": self.POSTGRES_PASSWORD,
#             "host": self.POSTGRES_HOST,
#             "port": self.POSTGRES_PORT,
#             "options": "-c search_path=content"
#         }
#
#
