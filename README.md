# Проект 7-го спринта

## Шаг 1. Обновить структуру Data Lake

### Структура будет такая

, как и спроектировали в 8-м уроке 2-й темы - три слоя:

- **STG** : /user/master/data/, https s3
  - /user/master/data/geo/events
  - /user/master/data/snapshots/
  - https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv

- **ODS** : /user/sergeibara/data/geo/
  - /user/sergeibara/data/geo/events
  - /user/sergeibara/data/geo/cities

  ```
  Cюда сливаем sample(0.05) данных из `/user/master/data/geo/events`, партиционируя по "date", "event_type" (?).

  Сюда же сливаем geo.csv

  Снапшоты (справочники) не переливаем в ODS, используем из STG.
  ```
  - Джобы для заливки ODS
    - /src/scripts/ods_fill_geo_events.py
    - /src/scripts/ods_fill_geo_cities.py

  - То же под Jupyter Notebook для RnD и отладки:
    - /src/scripts/ods_fill_geo_events_cell.py
    - /src/scripts/ods_fill_geo_cities_cell.py

- **Data Sandbox** : /user/sergeibara/analytics
  ```
  "на этом слое учебный проект и заканчивается", написано в 8-м уроке 2-й темы:
  "Вы остановитесь на этапе тестирования ваших витрин в песочнице.".
  Всё, что в ТЗ на проект названо "витрины", пишу в этот слой.
  ```

### Формат данных у нас такой:
|  |  |
|--|--|
| STG/events | `parquet` |
| STG/snapshots | `parquet` |
| STG/geo.csv | `csv` |
| ODS | `parquet` |
| Sandbox | `parquet` |
|  |  |

### Шаг 1.5. Выкачать на домашнюю машину датасет, созданный в ODS джобой как sample(0.05) от STG

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


## Шаг 2. Создать витрину в разрезе пользователей

...

```python
def event_with_city(path_event_prqt: str, path_city_data: str, spark: pyspark.sql.SparkSession) -> pyspark.sql.DataFrame:
    events_geo = sql.read.parquet(event_prqt) \
.sample(0.05) \
.drop('city','id') \
.withColumn('event_id', F.monotonically_increasing_id())
    
    city = sql.read.csv(path_city_data, sep = ";", header = True )
    
    events_city = events_geo \
.crossJoin(city) \
.withColumn('diff', F.acos(F.sin(F.col('lat_double_fin'))*F.sin(F.col('lat')) + F.cos(F.col('lat_double_fin'))*F.cos(F.col('lat'))*F.cos(F.col('lng_double_fin')-F.col('lng')))*F.lit(6371)).persist()
   
    return events_city 
```

```python
from datetime import datetime, date, timedelta
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['JAVA_HOME']='/usr'
# os.environ['SPARK_HOME'] ='/usr/lib/spark'
# os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

import sys
import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
# StructType, StructField, StringType, LongType, DecimalType, etc.:
from pyspark.sql.types import * 
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark import SparkFiles


def main():
    

    spark_app_name = f"ods_fill_geo_cities_cell"
    # .master("yarn") \
    # .master("local[8]") \
    # .master("local") \
    # NB: в spark-submit-версии master задастся при отправке файла
    spark = SparkSession.builder \
        .master("local") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", 2) \
        .appName(spark_app_name) \
        .getOrCreate()

    df_cities = spark.read.parquet("/user/sergeibara/data/geo/cities")
    print("df_cities")
    df_cities.show()
    df_cities.printSchema()
    
    events_pathes = (
        '/user/sergeibara/data/geo/events/date=2022-01-01',
        '/user/sergeibara/data/geo/events/date=2022-01-02',
        '/user/sergeibara/data/geo/events/date=2022-01-03',
        '/user/sergeibara/data/geo/events/date=2022-01-04',
        '/user/sergeibara/data/geo/events/date=2022-01-05',
        '/user/sergeibara/data/geo/events/date=2022-01-06',
        '/user/sergeibara/data/geo/events/date=2022-01-07'
    )
    df_events = spark.read \
                    .option("basePath", "/user/sergeibara/data/geo/events") \
                    .parquet(*events_pathes)\
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull()) \
        .withColumn('event_id', F.monotonically_increasing_id())
    print("df_events")
    #print(df_events.count())
    df_events.show()
    df_events.printSchema()
    
    window = Window.partitionBy("event_id").orderBy(F.asc("diff"))
    df_cross = df_events \
        .crossJoin(df_cities) \
        .withColumn(
            'diff',
            F.acos(F.sin(df_cities.lat)*F.sin(df_events.lat) + F.cos(df_cities.lat)*F.cos(df_events.lat)*F.cos(df_cities.lng-df_events.lon)) * F.lit(6371)
        ) \
        .withColumn('event_city', F.first('city', True).over(window)) \
        .drop(df_cities.lat) \
        .drop("id", "city", "lng", "diff") \
        .distinct()
    df_cross.show()


if __name__ == "__main__":
    main()
```