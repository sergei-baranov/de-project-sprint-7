# mart_fill_friends_cell.py
# NB: в spark-submit версии прописать комментами тут запуск из консоли
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
    start_date = datetime.strptime(date, "%Y-%m-%d")
    for n in range(depth):
        Ymd = (start_date + timedelta(n)).strftime("%Y-%m-%d")
        path = f"{base_path}/date={Ymd}"
        yield path


def input_paths(date, depth, base_path):
    return tuple(generate_dates_increase(date, depth, base_path))


def makeDfEvents(spark: pyspark.sql.SparkSession,
            path_events_src: str, deep_days: int) -> pyspark.sql.DataFrame:
    """
    Базовый датафрейм для всего остального.
    Для удобства и уменьшения места оставляем только необходимые для
    последующих операций поля, поднимая какие-то из них из event...
    на уровень выше, и переименовываем.

    Поля на выходе:

    event_id | processed_dttm | user_id | lat | lon | event_type | user_to_id | channel_id
    """
    log.info("path_events_src: {}".format(
        path_events_src
    ))

    if deep_days > 0:
        # под юпитер в master=local можно отработать
        # минимально достаточное для тз количество дней
        events_pathes = input_paths('2022-01-01', deep_days, path_events_src)
        msg = "events_pathes: {}...{}".format(
            events_pathes[0], events_pathes[-1]
        )
        print(msg)
        log.info(msg)

        df_events = spark.read \
                        .option("basePath", path_events_src) \
                        .parquet(*events_pathes)
    else:
        # в варианте под работающий (!) spark-submit --master yarn
        # передаём в deep_days 0 (ноль)
        df_events = spark.read.parquet(path_events_src)

    print("df_events")
    df_events.show()
    df_events.printSchema()

    df_base = df_events \
        .filter(F.col("event.message_from").isNotNull()) \
        .withColumn("event_id", F.monotonically_increasing_id()) \
        .withColumn("processed_dttm", F.col("date")) \
        .withColumn("user_id", F.col("event.message_from")) \
        .filter(F.col("user_id").isNotNull()) \
        .withColumn(
            "user_to_id",
            F.when(
                (
                    (F.col("event_type") == 'message')
                    & F.col("event.message_to").isNotNull()
                ),
                F.col("event.message_to")
            ).otherwise(F.lit(None))
        ) \
        .withColumn(
            "channel_id",
            F.when(
                F.col("event.message_channel_to").isNotNull(),
                F.col("event.message_channel_to")
            ) \
            .when(
                F.col("event.subscription_channel").isNotNull(),
                F.col("event.subscription_channel")
            ) \
            .when(
                F.col("event.channel_id").isNotNull(),
                F.col("event.channel_id")
            ) \
            .otherwise(F.lit(None))
        ) \
        .select(
            "event_id",
            "processed_dttm",
            "user_id",
            "lat",
            "lon",
            "event_type",
            "user_to_id",
            "channel_id"
        )

    return df_base


def makeDfUsersWithCoords(spark: pyspark.sql.SparkSession,
            df_base: pyspark.sql.DataFrame,
            path_cities_src: str) -> pyspark.sql.DataFrame:
    """
    Выбирает все квартеты "пользователь" + "дата" + "lat" + "lon"
    из всей совокупности событий.
    Обогащает ближайшим к координатам городом.
    Понадобится для пересечений пользователей по координатам на дату.
    Тип события значения не имеет, если есть координаты,
    значит фиксируем событие в выборке.

    Поля в датафрейме на входе: см. makeDfEvents
    Поля в датафрейме на выходе:
    user_id | processed_dttm | lat | lon | city
    """

    df_cities = spark.read.parquet(path_cities_src)

    print("df_cities")
    df_cities.show()
    df_cities.printSchema()

    window_city = Window.partitionBy("event_id").orderBy(F.asc("diff"))
    df_cross = df_base \
        .filter(
            (F.col("event_type") == "message")
            & F.col("lat").isNotNull()
            & F.col("lon").isNotNull()
        ) \
        .crossJoin(df_cities) \
        .withColumn(
            'diff',
            F.acos(F.sin(df_cities.lat)*F.sin(df_base.lat) + F.cos(df_cities.lat)*F.cos(df_base.lat)*F.cos(df_cities.lng-df_base.lon)) * F.lit(6371)
        ) \
        .withColumn('user_city', F.first('city', True).over(window_city)) \
        .select(
            F.col("user_id"),
            F.col("processed_dttm"),
            df_base.lat,
            F.col("lon"),
            F.col("user_city").alias("city")
        ) \
        .distinct()

    return df_cross


def makeDfUsersWithChannels(spark: pyspark.sql.SparkSession,
            df_base: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Выбирает все уникальные двойки "пользователь" + "канал" из совокупности
    channel_id is not null.
    Понадобится для пересечений пользователей по каналам.
    Дата значения не имеет.

    Поля в датафрейме на входе: см. makeDfEvents
    Поля в датафрейме на выходе:
    user_id | channel_id
    """

    #channel_admins_path = '/user/master/data/snapshots/channel_admins/actual'
    #channel_admins = sql.read.parquet(channel_admins_path)

    df = df_base \
        .filter(F.col("channel_id").isNotNull()) \
        .select(
            "user_id",
            "channel_id"
        ) \
        .distinct()

    return df;


def makeDfUsersWithAddressees(spark: pyspark.sql.SparkSession,
            df_base: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Выбирает все уникальные двойки "пользователь" + "адресат"
    из всей совокупности событий, у которых user_to_id is not null.
    Понадобится для отсечения уже общавшихся пользователей.
    Дата значения не имеет.

    Поля в датафрейме на входе: см. makeDfEvents
    Поля в датафрейме на выходе:
    user_left | user_right

    Пары формирую специально так, чтобы user_left был меньше user_right
    """

    # в user_left пишу сразу меньший ид, так как я далее
    # удаляю дублирующиеся пары через filter(left < right),
    # и соотв. джойнить этот датафрейм, который мы формируем,
    # буду с оставшейся парой
    df = df_base \
        .filter(F.col("user_to_id").isNotNull()) \
        .withColumn(
            "user_left",
            F.when(
                F.col("user_id") < F.col("user_to_id"),
                F.col("user_id")
            ).otherwise(
                F.col("user_to_id")
            )
        ) \
        .withColumn(
            "user_right",
            F.when(
                F.col("user_id") < F.col("user_to_id"),
                F.col("user_to_id")
            ).otherwise(
                F.col("user_id")
            )
        ) \
        .select(
            F.col("user_left"),
            F.col("user_right")
        ) \
        .distinct()

    return df;


def main():
    """
    deep_days - для ограничения выборки: сколько дней взять из данных от 2022-01-01,
    Для прод-запуска указать 0 (весь датасет).
    Для теста вполне норм работает 66 например (но это на sample(0.05)).

    nextdoor_kilometers: какое расстояние между пользователями считать за "рядом".
    Для теста работает 50 например.
    Для прод-а указать 1, согласно ТЗ.
    """
    # NB: в spark-submit версии вернуть параметризацию из аргументов запуска
    path_events_src = '/user/sergeibara/data/geo/events' # sys.argv[1]
    path_cities_src = '/user/sergeibara/data/geo/cities' # sys.argv[2]
    deep_days = 14 # int(sys.argv[3])
    nextdoor_kilometers = 50 # int(sys.argv[4])
    path_target = '/user/sergeibara/analytics/mart_friends' # sys.argv[5]

    spark_app_name = f"mart_fill_friends_cell_{deep_days}"
    # .master("yarn") \
    # .master("local[8]") \
    # .master("local") \
    # NB: в spark-submit-версии master задастся при отправке файла
    spark = SparkSession.builder \
        .master("local[8]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", 2) \
        .appName(spark_app_name) \
        .getOrCreate()

    # spark.stop()
    # spark.catalog.clearCache()

    df_base = makeDfEvents(spark, path_events_src, deep_days)
    print("df_base")
    df_base.show()
    df_base.printSchema()

    # print("df_base channel_id isNotNull")
    # df_base.filter(F.col("channel_id").isNotNull()).show()

    df_coords = makeDfUsersWithCoords(spark, df_base, path_cities_src)
    print("df_coords")
    df_coords.show()
    df_coords.printSchema()

    df_channels = makeDfUsersWithChannels(spark, df_base)
    print("df_channels")
    df_channels.show()
    df_channels.printSchema()

    df_friends = makeDfUsersWithAddressees(spark, df_base)
    print("df_friends")
    df_friends.show()
    df_friends.printSchema()

    # NB: как будем удалять дубли пар, которые являются дублями
    # вне зависимости от порядка элементов пары?
    # Если у нас гарантированно на каждую пару есть обратный дубль
    # (а у нас это так) - то просто:
    # .filter(F.col("user_left") < F.col("user_right")) - везде ниже
    # эта строчка прописана для удаления дублей пар.
    # (а так же датафрейм с парами из функции makeDfUsersWithAddressees()
    # заранее заточен под джойн только
    # по (user_left == user_left & user_right == user_right), а не
    # по (user_left == user_left & user_right == user_right)
    # | (user_left == user_right & user_right == user_left),
    # в них заранее left < right).
    #
    # Если бы не все записи были гарантированно парные,
    # то было бы с удалением дуюлей тяжелее, как-то так:
    # .withColumn("dpl", 
    #     F.when(
    #         F.col("user_left") < F.col("user_right"),
    #         F.concat(F.col("user_left"), F.lit("|"), F.col("user_right"))
    #     ).otherwise(
    #         F.concat(F.col("user_right"), F.lit("|"), F.col("user_left"))
    #     )
    # ) \
    # .dropDuplicates("dpl") \
    # .drop(F.col("dpl"))

    # Прежде всего по координатам определим пользователей,
    # которые в одну дату имели события с координатами ближе километра
    # (точнее - ближе nextdoor_kilometers).
    # NB: делаю скользкое допущение, но приемлемое, для ускорения,
    # что у двух событий близких по расстоянию будут одинаковые ближайшие города,
    # и добавляю условие (F.col("R.city") == F.col("L.city")).
    df_cities = spark.read.parquet(path_cities_src)
    df_nextdoors = df_coords.alias("L") \
        .join(
            df_coords.alias("R"),
            (
                (F.col("R.user_id") != F.col("R.user_id"))
                & (F.col("R.processed_dttm") == F.col("L.processed_dttm"))
                & (F.col("R.city") == F.col("L.city"))
            ),
            "inner"
        ) \
        .withColumn(
            'diff',
            F.acos(F.sin(F.col("R.lat"))*F.sin(F.col("L.lat")) + F.cos(F.col("R.lat"))*F.cos(F.col("L.lat"))*F.cos(F.col("R.lon") - F.col("L.lon"))) * F.lit(6371)
        ) \
        .filter(F.col("diff") <= nextdoor_kilometers) \
        .select(
            F.col("L.user_id").alias("user_left"),
            F.col("R.user_id").alias("user_right"),
            F.col("L.processed_dttm").alias("processed_dttm"),
            F.col("L.city").alias("city"),
            F.col("diff")
        ) \
        .filter(F.col("user_left") < F.col("user_right")) \
        .distinct() \
        .join(df_cities, "city", "inner") \
        .withColumnRenamed("id", "zone_id") \
        .drop("city", "lat", "lng") \
        .orderBy(F.col("diff").asc())
    print("df_nextdoors")
    df_nextdoors.show()
    df_nextdoors.printSchema()

    # теперь исключим из соседей тех, кто уже условно друзья
    df_not_friends = df_nextdoors \
        .join(
            df_friends,
            (
                (df_friends.user_left == df_nextdoors.user_left)
                & (df_friends.user_right == df_nextdoors.user_right)
            )
            #| (
            #    (df_friends.user_left == df_nextdoors.user_right)
            #    & (df_friends.user_right == df_nextdoors.user_left)
            #)
            ,
            "left_anti"
        )
    print("df_not_friends")
    df_not_friends.show()
    df_not_friends.printSchema()

    # !!! # каналов нет - в моей выборке sample(0.05) случилось так,
    # что нет ни одной записи с event.message_channel_to is not null
    # и т.п.
    # Для работоспособности проекта:
    # Делаю проверку, и если df_channels пустой, то просто беру первую запись
    # из df_not_friends, и пихаю user_left и user_right в df_channels,
    # назначая им один канал.
    if len(df_channels.head(1)) == 0:
        df_channels = df_channels.union(
            df_not_friends.limit(4).select(
                F.col("user_left").alias("user_id"),
                F.lit(1).alias("channel_id")
            )
        ).union(
            df_not_friends.limit(4).select(
                F.col("user_right").alias("user_id"),
                F.lit(1).alias("channel_id")
            )
        )
    print("df_channels")
    df_channels.show()
    df_channels.printSchema()

    # теперь датафрейм с каналами в любом случае заполнен,
    # и нам надо ограничить выборку только парами из него,
    # но сначала пары надо создать.
    # Обратим внимание, что датафрейм с каналами тоже заведомо парный,
    # соотв. дубли удалим через
    # .filter(F.col("user_left") < F.col("user_right")).
    df_channels_pairs = df_channels.alias("L") \
        .join(
            df_channels.alias("R"),
            (
                (F.col("R.channel_id") == F.col("L.channel_id"))
                & (F.col("R.user_id") != F.col("L.user_id"))
            ),
            "inner"
        ) \
        .select(
            F.col("L.user_id").alias("user_left"),
            F.col("R.user_id").alias("user_right")
        ) \
        .filter(F.col("user_left") < F.col("user_right")) \
        .distinct()
    print("df_channels_pairs")
    df_channels_pairs.show()
    df_channels_pairs.printSchema()

    # теперь из рядом находившихся в одну дату не-друзей
    # оставляем только таких, у которых есть общие каналы:
    df_recommend_pairs = df_not_friends \
        .join(
            df_channels_pairs,
            (
                (df_not_friends.user_left == df_channels_pairs.user_left)
                & (df_not_friends.user_right == df_channels_pairs.user_right)
            )
            #| (
            #    (df_not_friends.user_left == df_channels_pairs.user_right)
            #    & (df_not_friends.user_right == df_channels_pairs.user_left)
            #)
            ,
            "inner"
        ) \
        .drop(df_channels_pairs.user_left) \
        .drop(df_channels_pairs.user_right) \
        .distinct()
    print("df_recommend_pairs")
    df_recommend_pairs.show()
    df_recommend_pairs.printSchema()

    df_recommend_pairs.write \
        .mode("overwrite") \
        .parquet(path_target)

    dfTestRead = spark.read.parquet(path_target)
    print("dfTestRead")
    dfTestRead.show()
    dfTestRead.printSchema()

if __name__ == "__main__":
    main()
