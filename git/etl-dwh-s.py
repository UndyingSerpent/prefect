
### pip install psycopg2-binary
### pip install -U prefect
import psycopg2 # для работы с БД
from psycopg2 import Error # для обработки ошибок
from psycopg2.extras import execute_values
from prefect.blocks.system import Secret # секреты храним в prefect
from prefect import task, flow #оркестрация prefect
import datetime # работа со временем
        
# Задача для чтения данных из первой базы данных
@task
def extract():
    db_host = Secret.load("db-host")
    db_name = Secret.load("db-name")
    db_name_write = Secret.load("db-name-load")
    db_user = Secret.load("db-user")
    db_password = Secret.load("db-password")

    try:
        #пытаемся подключиться к базе
        conn_write = psycopg2.connect(dbname=db_name_write.get(), user=db_user.get(), password=db_password.get(), host=db_host.get(), port='6432')
        cur_write = conn_read.cursor()
        conn_read = psycopg2.connect(dbname=db_name.get(), user=db_user.get(), password=db_password.get(), host=db_host.get(), port='6432')
        cur_read = conn_read.cursor()
        
        #получим дату последней загрузки
        cur_write.execute("select max(rep.data_create_row) as maxdat from report_1 rep;")
        rows = cur_write.fetchone()
        print("Последняя загруженная дата - ", rows[0], "\n")
        last_load_date = rows
        
        #выберем новые записи для загрузки
        cur_read.execute("select * from report_1 rep where rep.data_create_row = %s", last_load_date)
        rows = cur_read.fetchall()
        print("Выбрано записей для заргузки - ", len(rows), "\n")

    except (Exception, Error) as error:
        # в случае сбоя подключения будет выведено сообщение в STDOUT
        print('Can`t establish connection to database', error)
    finally:
        if conn_read:
            cur_read.close()
            conn_read.close()
        if conn_write:
            cur_write.close()
            conn_write.close()
        return rows
    
# Задача для записи данных во вторую базу данных
@task
def load(rows):
    db_host = Secret.load("db-host")
    db_name = Secret.load("db-name")
    db_name_write = Secret.load("db-name-load")
    db_user = Secret.load("db-user")
    db_password = Secret.load("db-password")

    try:
        # Установка соединения со второй базой данных (запись)
        conn_write = psycopg2.connect(dbname=db_name_write.get(), user=db_user.get(), password=db_password.get(), host=db_host.get(), port='6432')
        cur_write = conn_write.cursor()
    
        # Начало транзакции для записи
        cur_write.execute("BEGIN")
    
        # Пакетная вставка записей во вторую таблицу
        insert_query = "INSERT INTO report_t1 VALUES %s"
        data_to_insert = [row for row in rows]
        execute_values(cur_write, insert_query, data_to_insert)

        # Завершение транзакции для записи
        conn_write.commit()
        print("Вставленно записей - ", len(rows), "\n")
    except Exception as e:
        # Откат изменений в случае ошибки
        conn_write.rollback()
        print("Ошибка транзакции при записи:", e)
    finally:
        # Закрытие соединений
        cur_write.close()
        conn_write.close()

# Создание Prefect Flow
@flow(log_prints=True)
def etl():
    data = extract()
    load(data)
    
if __name__ == "__main__":
    etl()
