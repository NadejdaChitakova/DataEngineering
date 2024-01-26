from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
import app 

default_args = {
        'owner' : 'airflow',
        'start_date' :datetime(2022, 11, 12),

}

dag = DAG(dag_id='DAG-MOVIE-DATA-TRANSFER',
        default_args=default_args,
        schedule_interval='@once', 
        catchup=False
    )

start = EmptyOperator(task_id = 'start', dag = dag)
end = EmptyOperator(task_id = 'end', dag = dag)

@task(task_id="ETL_MOVIE_DATA")
def etl_transfer_data(ds=None, **kwargs):
    app.execute_load()
    return "Success"

run_this = etl_transfer_data()

start >> run_this >> end