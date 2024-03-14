import random
from db import con
from datetime import datetime, timedelta

cursor_obj = con.cursor()
start_date = datetime(2000, 1, 1)
end_date = datetime(2000, 12, 31)

cursor_obj.execute("SELECT * FROM communities")
communities = cursor_obj.fetchall()

for community in communities:
    community_name = community[1] + "%"
    community_id = community[0]
    cursor_obj.execute(f"SELECT * FROM fields WHERE name LIKE '{community_name}'")
    fields = cursor_obj.fetchall()
    for field in fields:
        random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
        cursor_obj.execute("INSERT INTO fields_communities (field_id, community_id, createdAt, updatedAt) VALUES\
            (%s, %s, %s, %s)", (field[0], community_id, random_date, random_date))


con.commit()
cursor_obj.close()
con.close()