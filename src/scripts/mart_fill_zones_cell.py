# mart_fill_zones_cell.py
# NB: в spark-submit версии прописать комментами тут запуск из консоли
from datetime import datetime, timedelta
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['JAVA_HOME']='/usr'
# os.environ['SPARK_HOME'] ='/usr/lib/spark'
# os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

import findspark
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession
# StructType, StructField, StringType, LongType, DecimalType, etc.:
from pyspark.sql.types import * 
from pyspark.sql.window import Window
import pyspark.sql.functions as F


def generate_dates_increase(date, depth, base_path):
    start_date = datetime.strptime(date, "%Y-%m-%d")
    for n in range(depth):
        Ymd = (start_date + timedelta(n)).strftime("%Y-%m-%d")
        path = f"{base_path}/date={Ymd}"
        yield path


def input_paths(date, depth, base_path):
    return tuple(generate_dates_increase(date, depth, base_path))


def makeDfEventsSheet(spark: pyspark.sql.SparkSession,
                      path_events_src: str, path_cities_src: str,
                      path_mart_users_src: str,
                      deep_days: int) -> pyspark.sql.DataFrame:
    """
    Возвращает DataFrame, в котором каждому событию сопоставлен
    ближайший город (city, zone_id).
    Для событий, у которых нет широты или долготы, подставляется
    город из поля act_city витрины по пользователям.
    Дата события разложена на поля месяц и неделя от испокон веков
    (год * 100 + месяц, год * 100 + неделя года).
    Простыня обогащается типом события "registration"
    (самый ранний message пользователя).

    Все события отдаются простынёй, для последующего прогона оконками
    на количества записей.

    Поля в датафрейме:
    event_type | date | zone_id | week | month

    deep_days: мы знаем, что база стартует с 2022-01-01,
    и для ускорения разработки и отладки можем захотеть отработать
    не всю базу, а только deep_days от 2022-01-01;
    если явно передать 0 (ноль) - берётся вся база, что есть по path_events_src
    """
    df_cities = spark.read.parquet(path_cities_src)
    print("df_cities")
    df_cities.show()
    df_cities.printSchema()

    df_users = spark.read.parquet(path_mart_users_src)
    print("df_users")
    df_users.show()
    df_users.printSchema()

    if deep_days > 0:
        # под юпитер в master=local можно отработать
        # минимально достаточное для тз количество дней
        events_pathes = input_paths('2022-01-01', deep_days, path_events_src)
        print(events_pathes[0] + "..." + events_pathes[-1])

        df_events = spark.read \
                         .option("basePath", path_events_src) \
                         .parquet(*events_pathes)
    else:
        # в варианте под работающий (!) spark-submit --master yarn
        # передаём в deep_days 0 (ноль)
        print(path_events_src)
        df_events = spark.read.parquet(path_events_src)

    print("df_events")
    df_events.show()
    df_events.printSchema()

    # в данном случае будет asc_nulls_first,
    # будет F.first('city', False), а не True, это важно,
    # так как не будет .filter(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    window_city = Window.partitionBy("event_id").orderBy(F.asc_nulls_first("diff"))
    window_registration = Window.partitionBy("user_id").orderBy(F.asc("event.message_ts"))
    dfEvents = df_events \
        .withColumn('event_id', F.monotonically_increasing_id()) \
        .crossJoin(df_cities) \
        .withColumn(
            'diff',
            F.when(
                (df_events.lat.isNotNull() & df_events.lon.isNotNull()),
                F.acos(
                    F.sin(df_cities.lat) * F.sin(df_events.lat)
                    + F.cos(df_cities.lat) * F.cos(df_events.lat) * F.cos(df_cities.lng-df_events.lon)
                ) * F.lit(6371)
            ).otherwise(F.lit(None))
        ) \
        .withColumn('event_city', F.first('city', False).over(window_city)) \
        .withColumn("user_id", F.col("event.message_from")) \
        .withColumn(
            "registration_id",
            F.first("event_id", True).over(window_registration)
        ) \
        .withColumn(
            "registration",
            F.when(
                F.col("event_id") == F.col("registration_id"),
                F.lit(1)
            ).otherwise(F.lit(0))
        ) \
        .select(
            "user_id",
            "event_type",
            "event_city",
            "date",
            "registration"
        ) \
        .join(df_users, "user_id", "left") \
        .withColumn(
            "city",
            F.when(
                F.col("event_city").isNull(),
                F.col("act_city")
            ).otherwise(
                F.col("event_city")
            )
        ) \
        .filter(F.col("city").isNotNull()) \
        .cache()
    print("dfEvents")
    dfEvents.show()
    dfEvents.printSchema()

    dfRegistrations = dfEvents \
        .filter(F.col("registration") == 1) \
        .select(
            F.lit("registration").alias("event_type"),
            F.col("date"),
            F.col("city")
        ) \
        .cache()
    print("dfRegistrations")
    dfRegistrations.show()
    dfRegistrations.printSchema()

    dfSheet = dfEvents \
        .select(
            "event_type",
            "date",
            "city"
        ) \
        .union(dfRegistrations) \
        .join(df_cities, "city", "inner") \
        .select(
            "event_type",
            "date",
            df_cities.id.alias("zone_id")
        ) \
        .filter(F.col("zone_id").isNotNull()) \
        .withColumn(
            "week",
            F.year(F.col("date")) * 100 + F.weekofyear(F.col("date"))
        ) \
        .withColumn(
            "month",
            F.year(F.col("date")) * 100 + F.month(F.col("date"))
        ) \
        .cache()

    return dfSheet


def main():
    """
    deep_days - для ограничения выборки:
    сколько дней взять из данных от 2022-01-01,
    для прод-запуска указать 0?
    для теста вполне норм работает 66 например (но это на sample(0.05)).
    """
    # NB: в spark-submit версии вернуть параметризацию из аргументов запуска
    path_events_src = '/user/sergeibara/data/geo/events'  # sys.argv[1]
    path_cities_src = '/user/sergeibara/data/geo/cities'  # sys.argv[2]
    path_mart_users_src = '/user/sergeibara/analytics/mart_users'  # sys.argv[3]
    deep_days = 66  # int(sys.argv[4])
    path_target = '/user/sergeibara/analytics/mart_zones'  # sys.argv[5]

    spark_app_name = f"mart_fill_zones_cell_{deep_days}"

    spark = SparkSession.builder \
        .master("local[8]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", 2) \
        .appName(spark_app_name) \
        .getOrCreate()

    dfEventsSheet = makeDfEventsSheet(spark, path_events_src,
                                      path_cities_src, path_mart_users_src, deep_days)
    print("dfEventsSheet")
    dfEventsSheet.show()
    dfEventsSheet.printSchema()

    dfEventTypes = dfEventsSheet.select("event_type").distinct()
    print("dfEventTypes")
    dfEventTypes.show()
    dfEventTypes.printSchema()

    window_month = Window.partitionBy("zone_id", "month")
    dfMart = dfEventsSheet \
        .groupBy("zone_id", "month", "week") \
        .pivot(
            "event_type",
            ['message', 'reaction', 'subscription', 'registration']
        ) \
        .agg(F.count("*")) \
        .withColumnRenamed("message", "week_message") \
        .withColumnRenamed("reaction", "week_reaction") \
        .withColumnRenamed("subscription", "week_subscription") \
        .withColumnRenamed("registration", "week_user") \
        .withColumn("month_message", F.sum("week_message").over(window_month)) \
        .withColumn("month_reaction", F.sum("week_reaction").over(window_month)) \
        .withColumn("month_subscription", F.sum("week_subscription").over(window_month)) \
        .withColumn("month_user", F.sum("week_user").over(window_month)) \
        .orderBy(
            F.col("zone_id").asc(),
            F.col("month").asc(),
            F.col("week").asc()
        )
    print("dfMart")
    dfMart.show()
    dfMart.printSchema()

    dfMart.write \
        .mode("overwrite") \
        .parquet(path_target)

    dfTestRead = spark.read.parquet(path_target)
    print("dfTestRead")
    dfTestRead.show()
    dfTestRead.printSchema()


if __name__ == "__main__":
    main()
