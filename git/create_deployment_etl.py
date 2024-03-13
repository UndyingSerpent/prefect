from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/UndyingSerpent/prefect.git",
        entrypoint="./git/etl-dwh-s.py:etl",
    ).deploy(
        name="etl-dwh-s",
        work_pool_name="my-managed-pool",
        #job_variables={"pip_packages": ["pandas", "prefect-aws"]}
        job_variables={"pip_packages": ["psycopg2-binary"]}
        cron="0 7 * * *",
    )