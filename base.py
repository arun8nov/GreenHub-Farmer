import mysql.connector
import sqlalchemy
from dotenv import load_dotenv
import os 
import warnings
warnings.filterwarnings("ignore")

load_dotenv()

db_host = os.getenv("db_host")
db_port = os.getenv("db_port")
db_user = os.getenv("db_user")
db_password = os.getenv("db_password")
db_name = os.getenv("db_name")
reset_password = os.getenv("reset_db_password")

class Connections:
    def __init__(self):
        self
    
    def mysql_conn(self):
        conn = mysql.connector.connect(
        host = db_host,
        user = db_user,
        password = db_password,
        port = int(db_port),
        database = db_name
        )
        
        return conn
    
    def sql_engine(self):
        engine = sqlalchemy.create_engine(f"mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

        return engine
    
    def Reset_Databse(self,password):
    
        if password != reset_password :
            return "Please Enter Valid Password"
        
        else:
            try:
                conn = self.mysql_conn()
                cursor = conn.cursor()
                cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
                cursor.execute(f"USE {db_name}")
                conn.commit()

                return f"Database reset and created empty database {db_name}"
            except Exception as e:
                return f"Execution Failed due to database connection error {e}"
        