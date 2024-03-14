import random
from db import con

cursor_obj = con.cursor()

cursor_obj.execute("SELECT * FROM communities")
communities = cursor_obj.fetchall()

for community in communities:
    for i in range(random.randint(15, 30)):
        field_name = f"{community[1]}_field{i + 1}"
        field_size = random.randint(300, 1500)
        cursor_obj.execute("INSERT INTO fields (name, size, measurement_id) VALUES \
            (%s, %s, %s)", (field_name, field_size, 4))

con.commit()
cursor_obj.close()
con.close()