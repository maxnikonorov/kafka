# Приложение на Python с использованием Apache Kafka

Этот проект представляет собой простую реализацию Kafka

## Что делает? 
Producer забирает данные из PostgreSQl из таблицы user_logins
Consumer загружает данные в Clickhouse 

## Как запустить

### Шаг 1. Запускаем docker контейнер
Для этого в терминале пропишем 
```bash
docker-compose up -d
```

### Шаг 2. Установка зависимостей и окружения
* Создаем окружение для macos:
```bash
python3 -m venv venv
source venv/bin/activate
```
если windows:
```bash
python -m venv venv
venv\Scripts\activate.bat
```

* Установим библиотеки kafka, psycopg2, clickhouse_connect для корректной работы
```bash
pip install kafka-python psycopg2-binary clickhouse-connect
```

### Шаг 3. Запуск Продюсера
Откройте новое окно терминала и запустите продюсер для генерации и отправки тестовых данных:
```bash
python producer.py
```

### Шаг 4. Запуск Консьюмера
Откройте еще одно окно терминала и запустите скрипт консьюмера:
```bash
python consumer.py
```