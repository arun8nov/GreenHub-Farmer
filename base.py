import mysql.connector
import sqlalchemy
from dotenv import load_dotenv
import os 
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")

spark = SparkSession.builder \
    .appName("GreenHub") \
    .config(
        "spark.jars",
        "D:/spark_jars/mysql-connector-j-9.6.0.jar"
    ) \
    .getOrCreate()
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
        
class data_ingestion:
    def __init__(self):
        self.connections = Connections()
    
    def read_data(self):
        path = "raw_data\devices"
        sample_df = spark.read.parquet([f"{path}\{i}" for i in os.listdir(path)][0])
        df = sample_df.limit(0)
        for i in os.listdir(path):
            temp_df = spark.read.parquet(f"{path}\{i}")
            df = df.union(temp_df)
        
        return df

    def data_push(self):
        engine = self.connections.sql_engine()
        df = self.read_data()
        df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{db_host}:{db_port}/{db_name}") \
        .option("dbtable", "devices") \
        .option("user", db_user) \
        .option("password", db_password) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()

        return "Data Pushed Successfully"

        