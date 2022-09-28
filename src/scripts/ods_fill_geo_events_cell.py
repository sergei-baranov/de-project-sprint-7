# ods_fill_geo_events_cell.py
# НЕ ЗАПУСКАЙ. Не вариант в юпитере.
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


def stageEventsToOds(spark, path_src, path_target):
    df = spark.read.parquet(path_src).sample(0.05)
    print("read job is done")
    df.write \
            .partitionBy("date", "event_type") \
            .mode("overwrite") \
            .parquet(path_target)
    print("write job is done")

    return path_target


def main():
    path_src = "/user/master/data/geo/events" # sys.argv[1]
    path_target = "/user/sergeibara/data/geo/events" # sys.argv[2]

    spark_app_name = f"ods_fill_geo_events_cell"
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

    saved_path = stageEventsToOds(spark, path_src, path_target)

    print(f"done ({saved_path})")
    df = spark.read.parquet(saved_path)
    df.show()
    df.printSchema()


if __name__ == "__main__":
    main()