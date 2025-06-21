from airflow import DAG
from airflow.operators.bash import BashOperator


# Define default arguments for the DAG
default_args = {
    'owner': 'datath',
}

# Instantiate the DAG
with DAG(
    dag_id='simple_sequence_dag',
    default_args=default_args,
    description='A simple DAG demonstrating sequential tasks',
    tags=['example', 'simple'],
) as dag:
    # Task 1: Start message
    start_task = BashOperator(
        task_id='start_message',
        bash_command='echo "DAG started successfully!"',
    )

    # Task 2: Greet the world
    greet_task = BashOperator(
        task_id='greet_world',
        bash_command='echo "Hello, Airflow World!"',
    )

    # Task 3: Show current date and time
    show_datetime_task = BashOperator(
        task_id='show_current_datetime',
        bash_command='date',
    )

    # Task 4: End message
    end_task = BashOperator(
        task_id='end_message',
        bash_command='echo "DAG finished!"',
    )

    # Define the task dependencies
    start_task >> greet_task >> show_datetime_task >> end_task
