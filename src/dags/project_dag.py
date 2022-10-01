import airflow
from datetime import datetime, date, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

# базовые аргументы
default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

# DAG
dag_spark = DAG(
    dag_id = "project_dag",
    # запуск руками, т. к. учебный проект, учебная инфраструктура, etc.
    schedule_interval = None,
    default_args = default_args
)

init_phase = DummyOperator(
    task_id = 'init',
    dag = dag_spark
)

finish_phase = DummyOperator(
    task_id = 'finish',
    dag = dag_spark
)

# ods_fill_geo_cities_local = SparkSubmitOperator(
#     task_id = 'ods_fill_geo_cities',
#     dag = dag_spark,
#     application = '/lessons/scripts/ods_fill_geo_cities.py' ,
#     conn_id = 'yarn_spark',
#     application_args = [
#         # STG path to read
#         'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv',
#         # ODS path to write
#         '/user/sergeibara/data/geo/cities'
#     ],
#     conf = {
#         # "master": "yarn",
#         "spark.driver.maxResultSize": "20g"
#     },
#     executor_cores = 2,
#     executor_memory = '2g'
# )

# ods_fill_geo_events_local = SparkSubmitOperator(
#     task_id = 'ods_fill_geo_events',
#     dag = dag_spark,
#     application = '/lessons/scripts/ods_fill_geo_events.py' ,
#     conn_id = 'yarn_spark',
#     application_args = [
#         # STG path to read
#         '/user/master/data/geo/events',
#         # ODS path to write
#         '/user/sergeibara/data/geo/events'
#     ],
#     conf = {
#         # "master": "yarn",
#         "spark.driver.maxResultSize": "20g"
#     },
#     executor_cores = 2,
#     executor_memory = '2g'
# )

mart_fill_users_local = SparkSubmitOperator(
    task_id = 'mart_fill_users',
    dag = dag_spark,
    application = '/lessons/scripts/mart_fill_users.py' ,
    conn_id = 'yarn_spark',
    application_args = [
        # ODS path to read
        '/user/sergeibara/data/geo/events',
        # ODS path to read
        '/user/sergeibara/data/geo/cities',
        # сколько дней взять из данных от 2022-01-01
        # prod value: 0 (все)
        '66',
        # больше скольких дней без перерыва событий из города
        # делают его претендентом на "домашний город";
        # prod value: 27
        '7',
        # Sandbox path to write
        '/user/sergeibara/analytics/mart_users'
    ],
    conf = {
        # "master": "yarn",
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)

mart_fill_zones_local = SparkSubmitOperator(
    task_id = 'mart_fill_zones',
    dag = dag_spark,
    application = '/lessons/scripts/mart_fill_zones.py' ,
    conn_id = 'yarn_spark',
    application_args = [
        # ODS path to read
        '/user/sergeibara/data/geo/events',
        # ODS path to read
        '/user/sergeibara/data/geo/cities',
        # Sandbox path to read
        # (этот факт надо учитывать и не параллелить
        # таску с предыдущей (mart_fill_users_local))
        '/user/sergeibara/analytics/mart_users',
        # сколько дней взять из данных от 2022-01-01
        # prod value: 0 (все)
        '66',
        # Sandbox path to write
        '/user/sergeibara/analytics/mart_zones'
    ],
    conf = {
        # "master": "yarn",
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)

mart_fill_friends_local = SparkSubmitOperator(
    task_id = 'mart_fill_friends',
    dag = dag_spark,
    application = '/lessons/scripts/mart_fill_friends.py' ,
    conn_id = 'yarn_spark',
    application_args = [
        # ODS path to read
        '/user/sergeibara/data/geo/events',
        # ODS path to read
        '/user/sergeibara/data/geo/cities',
        # сколько дней взять из данных от 2022-01-01
        # prod value: 0 (все)
        '14',
        # какое расстояние между пользователями (в км) считать за "рядом"
        # prod value: 1
        '50',
        # Sandbox path to write
        '/user/sergeibara/analytics/mart_friends'
    ],
    conf = {
        # "master": "yarn",
        "spark.driver.maxResultSize": "20g"
    },
    executor_cores = 2,
    executor_memory = '2g'
)


# это закомментированная часть на заливку ODS из STG
# init_phase \
# >> [
#     ods_fill_geo_events_local,
#     ods_fill_geo_cities_local
# ] \

# это DAG чисто на витрины.
# mart_fill_friends_local логически можно параллелить с mart_fill_zones_local
# или mart_fill_users_local, но ресурс не позволит
init_phase \
>> mart_fill_users_local \
>> mart_fill_zones_local \
>> mart_fill_friends_local \
>> finish_phase
