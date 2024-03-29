# ods_fill_geo_events_cell.py
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
    path_src = '/user/master/data/geo/events'  # sys.argv[1]
    path_target = '/user/sergeibara/data/geo/events'  # sys.argv[2]

    spark_app_name = f"ods_fill_geo_events_cell"
    spark = SparkSession.builder \
        .master("local[8]") \
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
