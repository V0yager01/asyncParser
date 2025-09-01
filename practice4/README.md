
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
