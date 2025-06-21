from airflow.decorators import task, dag


default_args = {
    "owner": "datath",
}


@dag(default_args=default_args, tags=["example", "simple", "taskflow"])
def simple_taskflow_api_dag():
    @task()
    def start_message():
        print("DAG started successfully!")

    @task()
    def greet_world():
        print("Hello, Airflow World!")

    @task()
    def show_current_time():
        from datetime import datetime

        print(f"Current datetime: {datetime.now()}")

    @task()
    def end_message():
        print("DAG finished!")

    # Define the task dependencies using TaskFlow API
    start = start_message()
    greet = greet_world()
    show_dt = show_current_time()
    end = end_message()

    start >> greet >> show_dt >> end

simple_taskflow_api_dag()