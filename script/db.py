import psycopg2

con = psycopg2.connect(
database="task_manager",
user="my_user",
password="123",
host="localhost",
port= '5432'
)

cursor_obj = con.cursor()
cursor_obj.execute("SELECT * FROM states")
result = cursor_obj.fetchall()
print(result[-1])

cursor_obj.execute("SELECT * FROM users")
result = cursor_obj.fetchall()
print(result[-1])
