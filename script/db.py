import psycopg2

con = psycopg2.connect(
database="smart_farm",
user="farm",
password="123",
host="localhost",
port= '5432'
)