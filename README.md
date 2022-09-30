# Проект 7-го спринта

## Шаг 1. Обновить структуру Data Lake

### Структура будет такая

, как и спроектировали в 8-м уроке 2-й темы - три слоя:

- **STG** : /user/master/data/, https s3
  - `/user/master/data/geo/events`
  - `/user/master/data/snapshots/`
    - `/user/master/data/snapshots/channel_admins/actual`
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

## NB по глубине

Так как у нас учебные мощности и нерезиновое время, то отрабатываю код и логику на проекции реальных данных.

- В качестве ODS по событиям я заливаю sample(0.05) от STG.
- Почти все джобы сделал принимающими на вход аргумент deep_days, и могут выбирать deep_days из ODS по событиям, и по этому срезу дальше строят производные датафреймы.
Экспериментально я работал на deep_days = 66, и не очень мало, и относительно быстро.
- Джоба про домашний город принимает на вход количество дней, непрерывное нахождение в течение которых в городе делает его кандидатом на домашний город. По ТЗ 27, для скорости и наглядности я отрабатывал на значении 7, в основном.


## Шаг 2. Создать витрину в разрезе пользователей

- Джоба для теста например на Jupyter: `src/scripts/mart_fill_users_cell.py`
- Джоба для отправки в spark-submit --master yarn --deploy-mode cluster: `src/scripts/mart_fill_users.py`

NB: local_time не для всех act_city - см. комментарий в коде

```
'/user/sergeibara/data/geo/events' '/user/sergeibara/data/geo/cities' 66 7 '/user/sergeibara/analytics/mart_users'

+-------+----------+---------+--------------------+------------+--------------------+
|user_id|  act_city|home_city|        travel_array|travel_count|          local_time|
+-------+----------+---------+--------------------+------------+--------------------+
|  72592| Melbourne|     null|[Mackay, Launcest...|          12|2021-02-16 19:22:...|
|  72604| Melbourne|     null|         [Melbourne]|           1|2021-01-21 23:32:...|
|  72618| Melbourne|     null|         [Melbourne]|           1|2021-03-04 07:04:...|
|  72662| Toowoomba|     null|         [Toowoomba]|           1|                null|
|  72676|  Canberra|     null|          [Canberra]|           1|2021-02-11 18:14:...|
|  72688|   Bendigo|  Bendigo|[Bendigo, Newcast...|          11|                null|
|  72696| Newcastle|     null|         [Newcastle]|           1|                null|
|  72697|    Darwin|     null|            [Darwin]|           1|2021-01-06 12:36:...|
|  72702|   Bunbury|     null|           [Bunbury]|           1|                null|
|  72708|Launceston|     null|        [Launceston]|           1|                null|
|  72712| Newcastle|     null|         [Newcastle]|           1|                null|
|  72713|    Sydney|     null|            [Sydney]|           1|2021-02-17 16:49:...|
|  72718| Toowoomba|     null|         [Toowoomba]|           1|                null|
|  72748|    Sydney|     null|            [Sydney]|           1|2021-01-06 15:49:...|
|  72749| Newcastle|     null|         [Newcastle]|           1|                null|
|  72767|  Ballarat|     null|          [Ballarat]|           1|                null|
|  72769| Melbourne|     null|         [Melbourne]|           1|2021-01-29 18:05:...|
|  72786|   Bunbury|     null|  [Bendigo, Bunbury]|           2|                null|
|  72791|  Canberra|     null|          [Canberra]|           1|2021-02-05 18:27:...|
|  72799|    Sydney|     null|            [Sydney]|           1|2021-02-16 08:43:...|
+-------+----------+---------+--------------------+------------+--------------------+
```

## Шаг 3. Создать витрину в разрезе зон

- Джоба для теста например на Jupyter: `src/scripts/mart_fill_zones_cell.py`
- Джоба для отправки в spark-submit --master yarn --deploy-mode cluster: `src/scripts/mart_fill_zones.py`

```
mart_fill_zones.py '/user/sergeibara/data/geo/events' '/user/sergeibara/data/geo/cities' '/user/sergeibara/analytics/mart_users' 66 '/user/sergeibara/analytics/mart_zones'

+-------+------+------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
|zone_id| month|  week|week_message|week_reaction|week_subscription|week_user|month_message|month_reaction|month_subscription|month_user|
+-------+------+------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
|      4|202202|202207|         504|          456|             1560|      360|         1656|          2736|              5328|      1152|
|      4|202202|202208|         336|          960|             1560|      240|         1656|          2736|              5328|      1152|
|      9|202202|202206|        5640|         5400|            10224|     3936|        21240|         27816|             52488|     14520|
|      9|202202|202207|        5640|         7248|            14184|     3768|        21240|         27816|             52488|     14520|
|     18|202202|202206|         624|          672|             1344|      432|         2568|          3432|              6264|      1800|
|     18|202202|202207|         696|          864|             1944|      432|         2568|          3432|              6264|      1800|
|      6|202201|202202|         264|         null|              456|      240|         1200|           192|              1488|       840|
|      6|202201|202203|         264|           72|              144|      144|         1200|           192|              1488|       840|
|      6|202201|202205|          48|           24|               72|       24|         1200|           192|              1488|       840|
|      6|202201|202252|          96|         null|               72|       72|         1200|           192|              1488|       840|
|     11|202201|202201|        1272|         null|             1344|     1056|         7104|          1368|              5304|      4656|
|     11|202201|202202|        1536|           24|             1344|      888|         7104|          1368|              5304|      4656|
|     14|202201|202204|         144|           24|              120|       72|          840|            96|               984|       696|
|     14|202201|202205|          72|         null|               48|       72|          840|            96|               984|       696|
|      1|202201|202203|        2424|        34728|            60768|     1296|        10296|        125976|            666744|      5856|
|      1|202201|202204|        2568|        75144|           141840|     1104|        10296|        125976|            666744|      5856|
|      1|202202|202207|        2424|       214440|           414432|     1656|         9072|        790872|           1514664|      5880|
|      1|202202|202208|        1752|       266016|           511248|     1248|         9072|        790872|           1514664|      5880|
|      1|202203|202209|        1896|       287904|           546744|     1080|         2184|        338664|            645432|      1368|
|      1|202203|202210|         288|        50760|            98688|      288|         2184|        338664|            645432|      1368|
+-------+------+------+------------+-------------+-----------------+---------+-------------+--------------+------------------+----------+
```

## Шаг 4. Построить витрину для рекомендации друзей

- Джоба для теста например на Jupyter: `src/scripts/mart_fill_friends_cell.py`
- Джоба для отправки в spark-submit --master yarn --deploy-mode cluster: `src/scripts/mart_fill_friends.py`

```
mart_fill_friends.py '/user/sergeibara/data/geo/events' '/user/sergeibara/data/geo/cities' 14 100 '/user/sergeibara/analytics/mart_friends'

+---------+----------+--------------+------------------+-------+
|user_left|user_right|processed_dttm|              diff|zone_id|
+---------+----------+--------------+------------------+-------+
|    26826|    157859|    2022-01-04|16.032912509949394|      9|
|    66081|    106761|    2022-01-10|19.310661182272128|      9|
|    80045|     87725|    2022-01-01| 9.526481962831346|     22|
|    23696|    159764|    2022-01-14|19.038449479304987|      9|
+---------+----------+--------------+------------------+-------+
```

---
---
---

### Шаг 1.5. Сам себе: Как выкачать на домашнюю машину датасет, созданный в ODS джобой как sample(0.05) от STG

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