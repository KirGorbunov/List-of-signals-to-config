# Описание
Скрипт на основе списка сигналов создает файл-конфигуратор эмулятора и excel-файл для наполнения данными.

# Как запустить:
1. Скопировать в папку со скриптом excel-файл со списком сигналов (list_of_signals).
2. Изменить название файла .env.example на .env
3. Изменить имя excel-файла списка сигналов, например LIST_OF_SIGNALS_FILE=2402-03 List of signals.xlsx
4. Установить зависимости pip install -r requirements.txt
5. Запустить скрипт python script.py
6. В результате работы скрипта будут созданы файлы:
   - config.json - файл конфигурации эмулятора;
   - data.xlsx - файл с данными для эмуляции.