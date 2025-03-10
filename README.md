# List of signals to config

## Содержание
- [Описание](#описание)
- [Установка](#установка)
- [Использование](#использование)
- [Структура проекта](#структура-проекта)
- [Разработчикам](#разработчикам)

---

## Описание

`List of signals to config` — это скрипт, который на основе списка сигналов создает конфиг для эмулятора и excel-файл с шаблоном для датасета.

**ВАЖНО**: 
Результирующие файлы могут быть разделены по ассетам, настройки для разделения содержатся в .env файле:
- DIVIDE_CONFIG_BY_ASSET=True/False    # Разделять JSON-конфиги по оборудованию
- DIVIDE_DATA_BY_ASSET=True/False      # Разделять excel-файлы данных по оборудованию

---

## Установка

1. **Клонируйте репозиторий и перейдите в папку с приложением:**
    ```bash
    git clone git@github.com:bo-energo/mainbase.git
    cd mainbase
    cd List-of-signals-to-config
    ```

2. **Создайте виртуальное окружение:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Для Linux/MacOS
    venv\Scripts\activate   # Для Windows
    ```

3. **Установите зависимости из `requirements.txt`:**
    ```bash
    pip install -r requirements.txt
    ```

---

## Использование

1. **Скопировать в папку input_files excel-файл со списком сигналов (list_of_signals).**

2. **Скопировать .env.example и переименовать копию в .env:**
   ```bash
   cp .env.example .env
   ```

3. **В .env файле изменить имя excel-файла списка сигналов, например:**
    ```
    LIST_OF_SIGNALS_FILE=2402-03 List of signals.xlsx
    ```

4. **Запустить main.py в папке src:**
    ```bash
    python src/main.py
    ```
   В результате работы скрипта в папке ```output_files``` будут созданы файлы:
   - config_{asset}.json - файл(ы) конфигурации эмулятора;
   - data_{asset}.xlsx - шаблон(ы) excel-файла с данными для эмуляции.

---

## Структура проекта
``` 
├───input_files       # Папка, в которую необходимо поместить файл со списком сигналов
│    └───.gitkeep             # Номинальный файл для создания папки в репозитории
├───src               # Папка с исходным кодом
│    ├───data_loader.py       # Код загрузки данных из list of signals
│    ├───data_mapper.py       # Код создания маппингов
│    ├───file_creator.py      # Код создания файлов
│    ├───main.py              # Основной файл с кодом для запуска скрипта
│    ├───settings.py          # Настройки pydentic-settings
│    ├───signal_processor.py  # Код обработки данных
├── .env.example      # Пример .env-файла 
├── .gitignore        # Текстовый файл с перечнем файлов игнорируемых Git
├── poetry.lock       # Файл блокировки Poetry 
├── pyproject.toml    # Конфигурация Poetry 
├── README.md         # Документация
└── requirements.txt  # Зависимости для установки через pip
```
Папка output_files создается после запуска скрипта, в неё сохраняются результаты работы скрипта.

---

## Разработчикам

### Настройка окружения

**Установите зависимости с dev-зависимостями через Poetry:**
```bash
poetry install
```

### Линтинг
Для линтинга используйте инструмент `ruff`, который устанавливается автоматически через Poetry:
```bash
ruff check
```