from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 22),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('bronze_to_silver_dag', default_args=default_args, description='bronze to silver', schedule_interval='@daily', catchup=False) as dag :
    datagen_credits_bronze = SSHOperator(
    task_id='bronze_credits',  
    ssh_conn_id='spark_ssh_conn',  # SSH bağlantısı için belirtilen bağlantı kimliği
    conn_timeout = None,
    cmd_timeout = None,
    command=""" cd /dataops/data-generator && \
    source datagen/bin/activate && \
    python /dataops/data-generator/dataframe_to_s3.py \
    -buc tmdb-bronze \
    -k credit/credits_part \
    -aki dataops9 -sac Ankara06 \
    -eu http://minio:9000 \
    -i /dataops/tmdb_5000_movies_and_credits/tmdb_5000_credits.csv \
    -ofp True -z 500 -b 0.1 -oh True""")


    # SSHOperator kullanarak belirli bir SSH bağlantısı üzerinden komut çalıştırma
    datagen_movies_bronze = SSHOperator(task_id='bronze_movies',ssh_conn_id='spark_ssh_conn',  # SSH bağlantısı için belirtilen bağlantı kimliği
    conn_timeout = None,
    cmd_timeout = None,
    command=""" cd /dataops/data-generator && \
    source datagen/bin/activate && \
    python /dataops/data-generator/dataframe_to_s3.py \
    -buc tmdb-bronze \
    -k movies/movies_part \
    -aki dataops9 -sac Ankara06 \
    -eu http://minio:9000 \
    -i /dataops/tmdb_5000_movies_and_credits/tmdb_5000_movies.csv \
    -ofp True -z 500 -b 0.1 -oh True""")


    movies_s3_silver_task = SSHOperator(task_id='silver_movies', ssh_conn_id='spark_ssh_conn', conn_timeout = None, cmd_timeout = None,
    #source /dataops/airflowenv/bin/activate &&
    command=""" cd /dataops/airflowenv && \
    source bin/activate && \
    python /dataops/tmdb_movies_Tables_to_s3.py""")


    credits_s3_silver_task = SSHOperator(task_id='silver_credits', ssh_conn_id='spark_ssh_conn', conn_timeout = None, cmd_timeout = None,
    #source /dataops/airflowenv/bin/activate &&
    command=""" cd /dataops/airflowenv && \
    source bin/activate && \
    python /dataops/tmdb_credits_to_s3.py""")




    datagen_movies_bronze>>movies_s3_silver_task     
    datagen_credits_bronze>>credits_s3_silver_task
