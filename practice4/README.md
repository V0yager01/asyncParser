
## Запуcк
Создать файл .env
```
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres 
DB_NAME=postgres
DB_PASSWORD=example

```
Создайте таблицу
```
python create_db.py
```
В файле const.py укажите желаемое количество страниц (PAGE_COUNTS)
```
PAGE_COUNTS = 10
```
Запустите парсер
```
python main.py
```

## Производительность
В асинхронной версии скрипта время выполнения на 9 264 записей в бд составляет около 20 секунд.
В синхронной версии(https://github.com/V0yager01/module2/tree/master/task2) на то же количество записей в бд время выполнения составляет около 90 секунд.
