from airflow.decorators import dag, task
import pendulum

@dag(schedule=None, start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"), catchup=False)
def smoke_test():
    @task
    def check():
        import duckdb, pandas as pd
        print("duckdb", duckdb.__version__, "| pandas", pd.__version__)
        print(pd.read_csv("/usr/local/airflow/include/data/nammamart/stores.csv").shape)

    check()

smoke_test()