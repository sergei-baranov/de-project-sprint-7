# ods_fill_geo_cities_cell.py
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
    print("dfRus")
    dfRus.show()
    dfRus.printSchema()

    dfMath = dfRus.select(
        F.col("id"),
        F.col("city"),
        F.regexp_replace(F.col("lat"), ",", ".") \
            .cast(DecimalType(7, 4)).alias("lat"),
        F.regexp_replace(F.col("lng"), ",", ".") \
            .cast(DecimalType(7, 4)).alias("lng")
    )
    dfRus.unpersist()
    print("dfMath")
    dfMath.show()
    dfMath.printSchema()

    dfMath.write \
        .mode("overwrite") \
        .parquet(path_target)

    return path_target


def main():
    path_src_url = 'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv' # sys.argv[1]
    path_target = '/user/sergeibara/data/geo/cities' # sys.argv[2]

    spark_app_name = f"ods_fill_geo_cities_cell"
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

    saved_path = stageGeoToOds(spark, path_src_url, path_target)

    print(f"done ({saved_path})")
    df = spark.read.parquet(saved_path)
    df.show()
    df.printSchema()


if __name__ == "__main__":
    main()