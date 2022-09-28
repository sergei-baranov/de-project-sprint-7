# Проект 7-го спринта

## Шаг 1. Обновить структуру Data Lake

### Структура будет такая

, как и спроектировали в 8-м уроке 2-й темы - три слоя:

- **STG** : /user/master/data/, https s3
  - `/user/master/data/geo/events`
  - `/user/master/data/snapshots/`
  - `https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv`

- **ODS** : /user/sergeibara/data/geo/
  - `/user/sergeibara/data/geo/events` - события, перелитые из STG и раскиданные по партициям
  - `/user/sergeibara/data/geo/cities` - города с координатами

  ```
  Cюда сливаем sample(0.05) данных из `/user/master/data/geo/events`, партиционируя по "date", "event_type" (?).

  Сюда же сливаем geo.csv

  Снапшоты (справочники) не переливаем в ODS, используем из STG.

  Сюда же (возможно) персистируем какие-то промежуточные датафреймы, такие как сет сообщений с сопоставленными им ближайшими городами (?, если так будет быстрее, посмотрим).
  ```
  - Джобы для заливки ODS
    - `/src/scripts/ods_fill_geo_events.py`
    - `/src/scripts/ods_fill_geo_cities.py`

  - То же под Jupyter Notebook для RnD и отладки:
    - /src/scripts/ods_fill_geo_events_cell.py
    - /src/scripts/ods_fill_geo_cities_cell.py

- **Data Sandbox** : /user/sergeibara/analytics
  - `/user/sergeibara/analytics/mart_users` - витрина в разрезе пользователей
  - `/user/sergeibara/analytics/mart_zones` - витрина в разрезе зон
  - `/user/sergeibara/analytics/mart_friends` - витрина для рекомендации друзей
  ```
  "на этом слое учебный проект и заканчивается", написано в 8-м уроке 2-й темы:
  "Вы остановитесь на этапе тестирования ваших витрин в песочнице.".
  Всё, что в ТЗ на проект названо "витрины", пишу в этот слой.
  ```
  - Джобы для Data Sandbox:
    - `/src/scripts/mart_fill_users.py`
    - `/src/scripts/mart_fill_zones.py`
    - `/src/scripts/mart_fill_friends.py`

  - То же под Jupyter Notebook для RnD и отладки:
    - /src/scripts/mart_fill_users_cell.py
    - /src/scripts/mart_fill_zones_cell.py
    - /src/scripts/mart_fill_friends_cell.py

### Формат данных у нас такой:
|  |  |
|--|--|
| STG/events | `parquet` |
| STG/snapshots | `parquet` |
| STG/geo.csv | `csv` |
| ODS | `parquet` |
| Sandbox | `parquet` |
|  |  |

## Шаг 2. Создать витрину в разрезе пользователей

- Джоба для теста например на Jupyter - `src/scripts/mart_fill_users_cell.py`
- Джоба для отправки в spark-submit --master yarn --deploy-mode cluster - `src/scripts/mart_fill_users.py`

---
---
---

### Шаг 1.5. Сам себе: Выкачать на домашнюю машину датасет, созданный в ODS джобой как sample(0.05) от STG

На инфраструктуре:

```bash
...:/lessons$ hdfs dfs -ls /user/sergeibara/data/geo

Found 2 items
drwxr-xr-x   - sergeibara sergeibara          0 2022-09-27 16:20 /user/sergeibara/data/geo/cities
drwxr-xr-x   - sergeibara sergeibara          0 2022-09-27 14:40 /user/sergeibara/data/geo/events

...:/lessons$ hdfs dfs -ls /user/sergeibara/data/geo/events

Found 167 items
-rw-r--r--   1 sergeibara sergeibara          0 2022-09-27 14:40 /user/sergeibara/data/geo/events/_SUCCESS
drwxr-xr-x   - sergeibara sergeibara          0 2022-09-27 13:28 /user/sergeibara/data/geo/events/date=2022-01-01
drwxr-xr-x   - sergeibara sergeibara          0 2022-09-27 14:38 /user/sergeibara/data/geo/events/date=2022-01-02
drwxr-xr-x   - sergeibara sergeibara          0 2022-09-27 13:28 /user/sergeibara/data/geo/events/date=2022-01-03
...

...:/lessons$ hdfs dfs -copyToLocal /user/sergeibara/data/geo/events .
sergeibara@fhm73b7o1m6jhblpu73a:/lessons$ pwd
/lessons
sergeibara@fhm73b7o1m6jhblpu73a:/lessons$

```

На домашней машине:

```bash
...:~/YA_DE/SPRINT_9_Организация_Data_Lake$ sudo chmod 600 ./ssh_private_key
:~/YA_DE/SPRINT_9_Организация_Data_Lake$ ssh -i ./ssh_private_key yc-user@84.252.131.104
Welcome to Ubuntu 20.04.3 LTS (GNU/Linux 5.4.0-97-generic x86_64)
...

yc-user@...:~$ pwd
/home/yc-user
yc-user@...:~$ ls
yc-user@...:~$ docker ps
CONTAINER ID   IMAGE COMMAND         CREATED   STATUS PORTS NAMES
731613e149dd   ...   "/bin/sh ..."   9 hours   Up 9         student-sp7-1-sergeibara-0-3585701922
yc-user@...:~$
yc-user@...:~$ docker cp student-sp7-1-sergeibara-0-3585701922:/lessons/events .
yc-user@...:~$ ls
events
yc-user@...:~$ 
yc-user@...:~$ exit
logout
Connection to 84.252.131.104 closed.

...:~/YA_DE/SPRINT_9_Организация_Data_Lake$

...:~/YA_DE/SPRINT_9_Организация_Data_Lake$ scp -i ./ssh_private_key -r yc-user@84.252.131.104:events .

```

Обратно заливать аналогично, но лучше, думаю, включить шаг с упаковкой в .tar.