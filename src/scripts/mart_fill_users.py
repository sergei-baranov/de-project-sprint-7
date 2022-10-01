# mart_fill_users.py
# export HADOOP_CONF_DIR='/etc/hadoop/conf'
# export YARN_CONF_DIR='/etc/hadoop/conf'
# export JAVA_HOME='/usr'
# export SPARK_HOME='/usr/lib/spark'
# export PYTHONPATH='/usr/local/lib/python3.8'
# spark-submit --master yarn --deploy-mode cluster mart_fill_users.py '/user/sergeibara/data/geo/events' '/user/sergeibara/data/geo/cities' 0 27 '/user/sergeibara/analytics/mart_users'
# но запуск с yarn и cluster не особо работает, поэтому:
# spark-submit --master local[8] --deploy-mode client mart_fill_users.py '/user/sergeibara/data/geo/events' '/user/sergeibara/data/geo/cities' 66 7 '/user/sergeibara/analytics/mart_users'
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

import logging
log = logging.getLogger(__name__)


def generate_dates_increase(date, depth, base_path):
    """
    для ограничения работы по глубине формируем пути;
    это генератор
    """

    log.info("generate_dates_increase: '{}', '{}', '{}'".format(
        date, depth, base_path
    ))

    start_date = datetime.strptime(date, "%Y-%m-%d")
    for n in range(depth):
        Ymd = (start_date + timedelta(n)).strftime("%Y-%m-%d")
        path = f"{base_path}/date={Ymd}"
        yield path


def input_paths(date, depth, base_path):
    """
    для ограничения работы по глубине формируем пути;
    это оболочка к генератору
    """

    log.info("generate_dates_increase: '{}', '{}', '{}'".format(
        date, depth, base_path
    ))

    return tuple(generate_dates_increase(date, depth, base_path))


def makeDfEventsWithCities(spark: pyspark.sql.SparkSession,
            path_events_src: str, path_cities_src: str,
            deep_days: int) -> pyspark.sql.DataFrame:
    """
    Возвращает DataFrame, в котором каждому событию сопоставлен
    ближайший город (event_city).
    Выпилены события, у которых нет широты или долготы.

    deep_days: мы знаем, что база стартует с 2022-01-01,
    и для ускорения разработки и отладки можем захотеть отработать
    не всю базу, а только deep_days от 2022-01-01;
    если явно передать 0 (ноль) - берётся вся база, что есть по path_events_src
    """

    log.info("main: '{}', '{}', '{}'".format(
        path_events_src, path_cities_src, deep_days
    ))

    df_cities = spark.read.parquet(path_cities_src)

    if deep_days > 0:
        # под юпитер в master=local можно отработать
        # минимально достаточное для тз количество дней
        events_pathes = input_paths('2022-01-01', deep_days, path_events_src)
        # print(events_pathes[0] + "..." + events_pathes[-1])

        df_events = spark.read \
                        .option("basePath", path_events_src) \
                        .parquet(*events_pathes)
    else:
        # в варианте под работающий (!) spark-submit --master yarn
        # передаём в deep_days 0 (ноль)
        # print(path_events_src)
        df_events = spark.read.parquet(path_events_src)

    window = Window.partitionBy("event_id").orderBy(F.asc("diff"))
    df_cross = df_events \
        .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull()) \
        .withColumn('event_id', F.monotonically_increasing_id()) \
        .crossJoin(df_cities) \
        .withColumn(
            'diff',
            F.acos(F.sin(df_cities.lat)*F.sin(df_events.lat) + F.cos(df_cities.lat)*F.cos(df_events.lat)*F.cos(df_cities.lng-df_events.lon)) * F.lit(6371)
        ) \
        .withColumn('event_city', F.first('city', True).over(window)) \
        .drop(df_cities.lat) \
        .drop("id", "city", "lng", "diff") \
        .distinct() \
        .persist()

    return df_cross


def makeCohortsByDate(dfEvents: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    определяет актуальный город (act_city), время события, по которому определён
    актуальный город (act_city_event_ts), и строит когорты (поле grp_start_date)
    для дальнейшего определения домашнего города.

    возвращает DataFrame с полями
    user_id | event_date | event_city | act_city | act_city_event_ts | grp_start_date
    """

    log.info("makeCohortsByDate()")

    window_act_city = Window.partitionBy("user_id") \
                        .orderBy(F.desc("event.message_ts")) \
                        .rowsBetween(Window.unboundedPreceding, 1)
    window_serial_city = Window.partitionBy("user_id", "event_city") \
                        .orderBy(F.asc("event_date"))

    dfCohortsByDate = dfEvents \
        .withColumn("user_id", F.col("event.message_from")) \
        .withColumn("act_city", F.first("event_city", True).over(window_act_city)) \
        .withColumn("act_city_event_ts",
                        F.first("event.message_ts", True).over(window_act_city)) \
        .withColumn("event_date", F.to_date(F.col("event.message_ts"))) \
        .filter(F.col("event_date").isNotNull()) \
        .dropDuplicates(["user_id", "event_date", "event_city"]) \
        .select(
            "user_id",
            "event_date",
            "event_city",
            "act_city",
            "act_city_event_ts"
        ) \
        .orderBy(
            F.col("user_id").asc(),
            F.col("event_date").asc(),
            F.col("event_city").asc()
        ) \
        .withColumn("rn", F.row_number().over(window_serial_city)) \
        .withColumn("grp_start_date", F.expr("date_add(event_date, 0-rn)")) \
        .drop(F.col("rn")) \
        .cache()

    return dfCohortsByDate


def makeSeriesByTs(dfEvents: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    для вычислений путешествий готовим DataFrame на три поля:
    user_id | event_ts | event_city
    """

    log.info("makeSeriesByTs()")

    dfSeriesByTs = dfEvents \
        .withColumn("user_id", F.col("event.message_from")) \
        .withColumn("event_ts", F.col("event.message_ts")) \
        .filter(F.col("event_ts").isNotNull()) \
        .dropDuplicates(["user_id", "event_ts", "event_city"]) \
        .select(
            "user_id",
            "event_ts",
            "event_city"
        ) \
        .distinct() \
        .orderBy(
            F.col("user_id").asc(),
            F.col("event_ts").asc(),
            F.col("event_city").asc()
        ) \
        .cache()

    return dfSeriesByTs


def main():
    """
    deep_days и home_days - для ограничения выборки.
    deep_days - сколько дней взять из данных от 2022-01-01,
    home_days - больше скольких дней без перерыва событий из города
    делают его претендентом на "домашний город"
    для прод-запуска указать 0 и 27 соответственно.
    для теста вполне норм работает 66 и 7 например (но это на sample(0.05)).
    """

    path_events_src = sys.argv[1] # '/user/sergeibara/data/geo/events'
    path_cities_src = sys.argv[2] # '/user/sergeibara/data/geo/cities'
    deep_days = int(sys.argv[3]) # 66 (0 для полной отработки)
    home_days = int(sys.argv[4]) # 7 (по ТЗ 27, но см. на пред. параметр)
    path_target = sys.argv[5] # '/user/sergeibara/analytics/mart_users'

    log.info("main: '{}', '{}', '{}', '{}', '{}'".format(
        path_events_src, path_cities_src, deep_days,
        home_days, path_target
    ))

    spark_app_name = f"mart_fill_users_{deep_days}_{home_days}"
    # .master("yarn") \
    # .master("local[8]") \
    # .master("local") \
    # NB: в spark-submit-версии master задастся при отправке файла
    spark = SparkSession.builder \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", 2) \
        .appName(spark_app_name) \
        .getOrCreate()

    # события, обогащённые ближайшими городами (event_city),
    # и очищенные от таких, у которых нет координат
    dfEvents = makeDfEventsWithCities(spark, path_events_src,
                                path_cities_src, deep_days)

    # строим DataFrame user_id | act_city | act_city_event_ts | home_city
    dfCohortsByDate = makeCohortsByDate(dfEvents)

    window_last_home = Window.partitionBy("user_id", "event_city") \
                        .orderBy(F.desc("grp_start_date")) \
                        .rowsBetween(Window.unboundedPreceding, 1)

    dfHomes = dfCohortsByDate \
        .groupBy("user_id", "event_city", "grp_start_date") \
        .agg(F.count("*").alias('city_serial_dates_count')) \
        .filter(F.col("city_serial_dates_count") > home_days) \
        .withColumn("home_city",
            F.first("event_city", True).over(window_last_home)) \
        .drop("event_city", "grp_start_date", "city_serial_dates_count") \
        .distinct()

    dfHomeAndAct = dfCohortsByDate \
        .select("user_id", "act_city", "act_city_event_ts") \
        .distinct() \
        .join(dfHomes, "user_id", "left") \
        .orderBy(F.col("home_city").asc_nulls_last(), F.col("act_city").asc())

    # добавляем в DataFrame travel_count | travel_array

    # с путешествиями - надо построить теперь группы
    # по последовательным городам как мы строили по датам, НО
    # по event.message_ts, так как в одну дату австралиец
    # может посетить не один город
    dfSeriesByTs = makeSeriesByTs(dfEvents)

    win_prev_city = Window.partitionBy("user_id") \
                        .orderBy(F.asc("event_ts"))
    win_travel_city = Window.partitionBy("user_id") \
                        .orderBy(F.asc("event_ts")) \
                        .rowsBetween(
                            Window.unboundedPreceding,
                            Window.unboundedFollowing
                        )
    dfTravels = dfSeriesByTs \
        .withColumn("lag_city", F.lag("event_city", 1).over(win_prev_city)) \
        .filter(
            (F.col("event_city") != F.col("lag_city"))
            | F.col("lag_city").isNull()
        ) \
        .orderBy(F.col("user_id").asc(), F.col("event_ts").asc()) \
        .withColumn("travel_array",
            F.collect_list('event_city').over(win_travel_city)) \
        .withColumn("travel_count", F.size(F.col("travel_array"))) \
        .select(
            F.col("user_id"),
            F.col("travel_array"),
            F.col("travel_count")
        ) \
        .distinct() \
        .orderBy(F.col("travel_count").desc(), F.col("user_id").asc())

    # добавляем в витрину local_time.
    # тут не очень понятно. у нас в витрине всё сто раз агрегировано,
    # относительно чего считать время? событий как таковых в витрине не осталось.
    # Буду считать, что local_time от события, которое дало нам act_city,
    # для этого выше рядом с act_city добавил поле act_city_event_ts.
    # 
    # Так же учтём резерч от однокашницы в slack-е:
    # 
    # Irina Orlova
    # В общем, я поисследовала тему с local time.
    # Получилось вот что: судя вот по этой статье,
    # Spark использует для идентификации таймзон Internet Assigned Numbers
    # Authority Time Zone Database (IANA TZDB).
    # Я ее скачала и увидела, что там не каждый город можно преобразовать в таймзону.
    # Насчитала вот эти зоны:
    # Australia/Darwin
    # Australia/Perth
    # Australia/Eucla
    # Australia/Brisbane
    # Australia/Lindeman
    # Australia/Adelaide
    # Australia/Hobart
    # Australia/Melbourne
    # Australia/Sydney
    # Australia/Broken_Hill
    # Australia/Lord_Howe
    # Также еще работает Australia/Canberra.
    #
    # Потому создадим ещё датафрейм с валидными городами, и только для них
    # и будем выводить local_time.
    # Irina Orlova считала ближайшие расстояния, но времени у меня на это нет уже,
    # к тому же ближайший по расстоянию - не значит в той же таймзоне,
    # они же по долгте только идут по широте одинаковые,
    # плюс административные границы. Всё сложнее ).
    data = [
        ('Canberra', 'Australia/Canberra'),
        ('Darwin', 'Australia/Darwin'),
        ('Perth', 'Australia/Perth'),
        ('Eucla', 'Australia/Eucla'),
        ('Brisbane', 'Australia/Brisbane'),
        ('Lindeman', 'Australia/Lindeman'),
        ('Adelaide', 'Australia/Adelaide'),
        ('Hobart', 'Australia/Hobart'),
        ('Melbourne', 'Australia/Melbourne'),
        ('Sydney', 'Australia/Sydney'),
        ('Broken Hill', 'Australia/Broken_Hill'),
        ('Lord Howe', 'Australia/Lord_Howe')
    ]
    columns = StructType([
        StructField("city", StringType(), nullable=False),
        StructField("tz", StringType(), nullable=False)]
    )
    dfTimezones = spark.createDataFrame(data=data, schema=columns)
    dfMart = dfHomeAndAct \
            .join(dfTravels, "user_id", "left") \
            .join(dfTimezones, dfHomeAndAct.act_city == dfTimezones.city, "left") \
            .withColumn(
                "local_time",
                F.when(F.col("tz").isNotNull(),
                    F.from_utc_timestamp(
                        F.col("act_city_event_ts").cast("Timestamp"),
                        F.col("tz")
                    )
                ).otherwise(F.lit(None))
            ) \
            .drop("act_city_event_ts", "city", "tz") \
            .orderBy(F.col("user_id").asc())

    dfMart.write \
        .mode("overwrite") \
        .parquet(path_target)

    # dfTestRead = spark.read.parquet(path_target)
    # print("dfTestRead")
    # dfTestRead.show()
    # dfTestRead.printSchema()

if __name__ == "__main__":
    main()