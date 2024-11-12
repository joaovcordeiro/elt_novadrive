from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='postgres_to_snowflake_upsert',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake using DATA_ATUALIZACAO with upsert',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    concurrency=1
)
def postgres_to_snowflake_etl():
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']

    @task
    def get_max_data_atualizacao(table_name: str):
        logging.info(f"Getting max DATA_ATUALIZACAO for table: {table_name}")
        with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT MAX(DATA_ATUALIZACAO) FROM {table_name}")
                max_data_atualizacao = cursor.fetchone()[0]
                return max_data_atualizacao if max_data_atualizacao is not None else datetime(datetime.now().year, 1, 1)

    @task
    def load_incremental_data(table_name: str, last_update: datetime):
        logging.info(f"Loading incremental data for table: {table_name} with last update: {last_update}")
        try:
            with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn:
                with pg_conn.cursor() as pg_cursor:
                    pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                    columns = [row[0] for row in pg_cursor.fetchall()]
                    columns_list_str = ', '.join(columns)

                    pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE DATA_ATUALIZACAO > %s", (last_update,))
                    rows = pg_cursor.fetchall()

                    if rows:
                        with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as sf_conn:
                            with sf_conn.cursor() as sf_cursor:
                                try:
                                    sf_cursor.execute("BEGIN")
                                    logging.info(f"Transaction started for table: {table_name}")

                                    for row in rows:
                                        merge_query = f"""
                                        MERGE INTO {table_name} AS target
                                        USING (SELECT {', '.join([f'%s AS {col}' for col in columns])}) AS source
                                        ON target.ID_{table_name} = source.ID_{table_name}
                                        WHEN MATCHED THEN
                                            UPDATE SET {', '.join([f'target.{col} = source.{col}' for col in columns])}
                                        WHEN NOT MATCHED THEN
                                            INSERT ({columns_list_str})
                                            VALUES ({', '.join(['source.' + col for col in columns])});
                                        """
                                        sf_cursor.execute(merge_query, row)
                                    logging.info(f"Upsert completed for {len(rows)} rows into Snowflake for table: {table_name}")

                                    sf_cursor.execute("COMMIT")
                                    logging.info(f"Transaction committed for table: {table_name}")
                                except Exception as e:
                                    sf_cursor.execute("ROLLBACK")
                                    logging.error(f"Transaction rolled back for table: {table_name} due to error: {e}")
                                    raise
        except Exception as e:
            logging.error(f"Error loading data for {table_name}: {e}")
            raise

    for table_name in table_names:
        last_update = get_max_data_atualizacao(table_name)
        load_incremental_data(table_name, last_update)

postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()
