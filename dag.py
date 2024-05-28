from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'dibimbing',
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'spark_retail_analysis',
    default_args=default_args,
    description='A Spark job to analyze retail data',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
)

spark_master = Variable.get("spark_master", default_var="spark://spark-master:7077")
spark_jars = Variable.get("spark_jars", default_var="/tmp/docker-desktop-root/mnt/host/c/Users/Muthia/Downloads/scrapt/dibimbing_spark_airflow-main/postgresql-42.2.18.jar")
spark_script = Variable.get("spark_script", default_var="/tmp/docker-desktop-root/mnt/host/c/Users/Muthia/Downloads/scrapt/dibimbing_spark_airflow-main/pyspark_script.py")
db_url = Variable.get("db_url", default_var="jdbc:postgresql://localhost:5432/postgres")
db_user = Variable.get("db_user", default_var="postgres")
db_password = Variable.get("db_password", default_var="26111999Map")

submit_spark_job = SparkSubmitOperator(
    task_id='submit_job',
    application=spark_script,
    conn_id='spark_default',
    jars=spark_jars,
    driver_class_path=spark_jars,
    java_class='org.apache.spark.examples.SparkPi', 
    dag=dag,
    env_vars={
        'DB_URL': db_url,
        'DB_USER': db_user,
        'DB_PASSWORD': db_password,
        'SPARK_JARS': spark_jars
    }
)

submit_spark_job
