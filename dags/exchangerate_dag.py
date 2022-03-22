from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from exchangerate.scripts import merge_procedure

default_args = {
    'owner': 'airflow'
}

with DAG('exchangerate',
         default_args=default_args,
         schedule_interval='0 0/3 * * *',
         start_date=datetime(2022, 3, 22),
         tags=['exchangerate', 'assignment'],
         catchup=False,
         max_active_runs=1
         ) as dag:
    @task.virtualenv(
        task_id="check_state", requirements=["peewee==3.14.10"], system_site_packages=True
    )
    def load_data():
        from exchangerate.exchangerate import check_state, load_recent_pairs, load_history_pairs
        from airflow.models import Variable
        db_conf = Variable.get("exchangerate_db_secret", deserialize_json=True)
        pairs = Variable.get("pairs", deserialize_json=True)

        print('going to check {} in database'.format(pairs))
        pairs_to_load_recent, pairs_to_load_history = check_state(db_conf, pairs)

        print('going to update pairs: {}'.format(pairs_to_load_recent))
        load_recent_pairs(db_conf, pairs_to_load_recent)
        print('pairs update finished')

        print('going to load pairs to load from scratch: {}'.format(pairs_to_load_history))
        load_history_pairs(db_conf, pairs_to_load_history)
        print('historical load finished')


    load = load_data()

    merge = PostgresOperator(
        task_id='merge_sql',
        postgres_conn_id='exchangerate_db',
        sql=merge_procedure
    )

    load >> merge
