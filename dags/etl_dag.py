from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import timedelta
import mysql.connector 
import requests
import json
import datetime

def create_connection():
    conn = None
    cur = None
    try:
        conn = mysql.connector.connect(
            host="c-mysql",
            port=3306,
            user="root",
            password="123",
            db="demo_airflow"
        )
        cur = conn.cursor()
        print(f"Connect to database {conn.database} successfully!!!")
    except Exception as e:
        print("Error :", e)
    return conn, cur


default_args = {
    "owner" : 'nguyennam',
    "start_date" : datetime.datetime.utcnow() - timedelta(minutes=1),
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2),
    "depends_on_past" : False,
}
@dag(
    default_args=default_args,
    schedule_interval="*/1 * * * *",
    catchup=False,
)
def etl_dag():
    @task(task_id="create_tables")
    def create_table(conn, cur):
        sql_query_create_product = """create table if not exists products(id int primary key,
                                            title varchar(100),
                                            description text,
                                            price float,
                                            discountPercentage float,
                                            rating float,
                                            stock float,
                                            brand varchar(100),
                                            category varchar(100),
                                            updated_time datetime);"""
        sql_query_create_user = """create table if not exists users(id int primary key,
                                            firstName varchar(100),
                                            lastName varchar(100),
                                            maidenName varchar(100),
                                            age int,
                                            gender char(10),
                                            email varchar(100),
                                            phone varchar(100),
                                            username varchar(100),
                                            password varchar(100),
                                            birthDate date,
                                            updated_time datetime
                                            );"""
        cur.execute(sql_query_create_product)
        cur.execute(sql_query_create_user)
        conn.commit()

    @task(task_id="extract")
    def extract(conn, cur, name_db):
        cur.execute("show tables;")
        tables = [t[0] for t in cur.fetchall()]
        if name_db not in tables:
            latest_id = 0
        else:
            cur.execute(f"select max(id) from {name_db}")
            id = cur.fetchone()[0]
            if id == None:
                latest_id = 0
            else:
                latest_id = id
        r = requests.get(f"https://dummyjson.com/{name_db}?limit=10&skip={latest_id}")
        if r.status_code == 200:
            data = json.loads(r.content)
        else:
            print("Status code:",r.status_code)
            return
        return data
    
    @task(task_id="load_product")
    def load_product(conn, cur, data_product):
        dict_product = data_product["products"]
        for product in dict_product:
            id = product["id"]
            title = product["title"]
            description = product["description"]
            price = product["price"]
            discountPercentage = product["discountPercentage"]
            rating = product["rating"]
            stock = product["stock"]
            brand = product["brand"]
            category = product["category"]
            updated_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql_insert = """insert into products(id, title, description, price, discountPercentage, rating, stock, brand, category, updated_time)
                            values(%s, %s, %s , %s, %s, %s, %s, %s, %s, %s)"""
            val_insert = (id, title, description, price, discountPercentage, rating, stock, brand, category, updated_time)
            cur.execute(sql_insert, val_insert)
            conn.commit()

    @task(task_id="load_user")
    def load_user(conn, cur, data_user):
        dict_user = data_user["users"]
        for user in dict_user:
            id = user["id"]
            firstName = user["firstName"]
            lastName = user["lastName"]
            maidenName = user["maidenName"]
            age = user["age"]
            gender = user["gender"]
            email = user["email"]
            phone = user["phone"]
            username = user["username"]
            password = user["password"]
            birthDate = user["birthDate"]
            updated_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            sql_insert = """insert into users(id, firstName, lastName, maidenName, age, gender, email, phone, username, password, birthDate, updated_time)
                            values(%s, %s, %s , %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            val_insert = (id, firstName, lastName, maidenName, age, gender, email, phone, username, password, birthDate, updated_time)
            cur.execute(sql_insert, val_insert)
            conn.commit()

    conn, cur = create_connection()
    create_table(conn=conn, cur=cur)
    data_product = extract(conn=conn, cur=cur, name_db="products")
    data_user = extract(conn=conn, cur=cur, name_db="users")
    load_product(conn=conn, cur=cur, data_product=data_product)
    load_user(conn=conn, cur=cur, data_user=data_user)
dag = etl_dag()
    

    

    