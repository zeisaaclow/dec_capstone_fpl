from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(dag_id='run_fpl_elt',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id='trigger_airbyte',
        airbyte_conn_id='airbyte_fpl',
        connection_id='b2ababed-1d11-469e-838e-024e7cbf7164',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    dbt_version = BashOperator(
        task_id="dbt_version",
        bash_command="/usr/local/airflow/dbt_env/bin/dbt --version",
    )

    dbt_env_json = Variable.get("DBT_ENV", deserialize_json=True)

    print(dbt_env_json)

    dbt_build = BashOperator(
        task_id="dbt_build",
        env=dbt_env_json,
        bash_command="cp -R /opt/airflow/dags/dbt /tmp;\
            cd /tmp/dbt/fpl_analytics;\
            /usr/local/airflow/dbt_env/bin/dbt deps;\
            /usr/local/airflow/dbt_env/bin/dbt build --project-dir /tmp/dbt/fpl_analytics/ --profiles-dir . --target dev;",
    )

    airbyte_sync >> dbt_version >> dbt_build