from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/UndyingSerpent/prefect.git",
        entrypoint="etl-dwh-s.py:etl",
    ).deploy(
        name="etl-dwh-s",
        work_pool_name="my-managed-pool",
        cron="0 7 * * *",
    )