# ods_fill_geo_events.py
# export HADOOP_CONF_DIR='/etc/hadoop/conf'
# export YARN_CONF_DIR='/etc/hadoop/conf'
# export JAVA_HOME='/usr'
# export SPARK_HOME='/usr/lib/spark'
# export PYTHONPATH='/usr/local/lib/python3.8'
# spark-submit --master yarn --deploy-mode cluster ods_fill_geo_events.py '/user/master/data/geo/events' '/user/sergeibara/data/geo/events'
# но запуск с yarn и cluster не особо работает, поэтому:
# spark-submit --master local[8] --deploy-mode client ods_fill_geo_events.py '/user/master/data/geo/events' '/user/sergeibara/data/geo/events'
import findspark
findspark.init()
findspark.find()

from datetime import datetime, date, timedelta
import os
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
# os.environ['JAVA_HOME']='/usr'
# os.environ['SPARK_HOME'] ='/usr/lib/spark'
# os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

import sys

import pyspark
from pyspark.sql import SparkSession
# StructType, StructField, StringType, LongType, DecimalType, etc.:
from pyspark.sql.types import * 
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark import SparkFiles


def stageEventsToOds(spark, path_src, path_target):
    """
    Переливаем из path_src sample(0.05) данных
    в path_target, партиционируя по "date", "event_type"
    """
    df = spark.read.parquet(path_src).sample(0.05)
    print("read job is done")
    df.write \
            .partitionBy("date", "event_type") \
            .mode("overwrite") \
            .parquet(path_target)
    print("write job is done")

    return path_target


def main():
    path_src = sys.argv[1] # '/user/master/data/geo/events'
    path_target = sys.argv[2] # '/user/sergeibara/data/geo/events'

    spark_app_name = f"ods_fill_geo_events"
    # .master("yarn") \
    # .master("local[8]") \
    # .master("local") \
    # NB: в spark-submit-версии master задаётся при отправке файла
    spark = SparkSession.builder \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", 2) \
        .appName(spark_app_name) \
        .getOrCreate()

    saved_path = stageEventsToOds(spark, path_src, path_target)

    print(f"done ({saved_path})")
    df = spark.read.parquet(saved_path)
    df.show()
    df.printSchema()


if __name__ == "__main__":
    main()