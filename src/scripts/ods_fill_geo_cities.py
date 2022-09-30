# ods_fill_geo_cities.py
# export HADOOP_CONF_DIR='/etc/hadoop/conf'
# export YARN_CONF_DIR='/etc/hadoop/conf'
# export JAVA_HOME='/usr'
# export SPARK_HOME='/usr/lib/spark'
# export PYTHONPATH='/usr/local/lib/python3.8'
# spark-submit --master yarn --deploy-mode cluster ods_fill_geo_cities.py 'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv' '/user/sergeibara/data/geo/cities'
# но запуск с yarn и cluster не особо работает, поэтому:
# spark-submit --master local[8] --deploy-mode client ods_fill_geo_cities.py 'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv' '/user/sergeibara/data/geo/cities'
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


def stageGeoToOds(spark, path_src_url, path_target):
    geoSchemaRus = StructType([
        StructField("id", LongType(), nullable=False),
        StructField("city", StringType(), nullable=False),
        StructField("lat", StringType(), nullable=False),
        StructField("lng", StringType(), nullable=False)]
    )

    spark.sparkContext.addFile(path_src_url)

    #  NB: RnD-чтение - вместо схемы делай inferShema=True
    dfRus = spark \
        .read \
        .option("delimiter", ";") \
        .csv("file://" + SparkFiles.get("geo.csv"),
                            header=True, schema=geoSchemaRus)

    dfMath = dfRus.select(
        F.col("id"),
        F.col("city"),
        F.regexp_replace(F.col("lat"), ",", ".") \
            .cast(DecimalType(7, 4)).alias("lat"),
        F.regexp_replace(F.col("lng"), ",", ".") \
            .cast(DecimalType(7, 4)).alias("lng")
    )
    dfRus.unpersist()

    dfMath.write \
        .mode("overwrite") \
        .parquet(path_target)

    return path_target


def main():
    path_src_url = sys.argv[1] # "https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv"
    path_target = sys.argv[2] # "/user/sergeibara/data/geo/cities"

    spark_app_name = f"ods_fill_geo_cities"
    # .master("yarn") \
    # .master("local[8]") \
    # .master("local") \
    # в spark-submit-версии master задаётся при отправке файла
    spark = SparkSession.builder \
        .config("spark.driver.memory", "2g") \
        .config("spark.driver.cores", 2) \
        .appName(spark_app_name) \
        .getOrCreate()

    saved_path = stageGeoToOds(spark, path_src_url, path_target)


if __name__ == "__main__":
    main()