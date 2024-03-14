import random
from db import con
from consts import *

class DataMocking:
    def __init__(self):
        self.cursor_obj = con.cursor()
    
    def fields(self):
        self.cursor_obj.execute("SELECT * FROM communities")
        communities = self.cursor_obj.fetchall()

        for community in communities:
            for i in range(random.randint(min_field_count, max_field_count)):
                field_name = f"{community[1]}_field{i + 1}"
                field_size = random.randint(min_field_size, max_field_size)
                self.cursor_obj.execute("INSERT INTO fields (name, size, measurement_id) VALUES \
                    (%s, %s, %s)", (field_name, field_size, 4))

        for community in communities:
            community_name = community[1] + "%"
            community_id = community[0]
            self.cursor_obj.execute(f"SELECT * FROM fields WHERE name LIKE '{community_name}'")
            fields = self.cursor_obj.fetchall()
            for field in fields:
                self.cursor_obj.execute("INSERT INTO fields_communities (field_id, community_id) VALUES\
                    (%s, %s)", (field[0], community_id))


    def portable_devices(self):
        data = (
            "BelAZ 75710", "KAMAZ-65225", "MAZ-6501", "KrAZ-7140H6", "URAL-5557",
            "BelAZ 75302", "KAMAZ-65228", "MAZ-6516", "KrAZ-65101", "URAL-4320",
            "BelAZ 7555", "KAMAZ-65801", "MAZ-6517", "KrAZ-65053", "URAL-55571",
            "BelAZ 75131", "KAMAZ-65115", "MAZ-65115", "KrAZ-6322", "URAL-55572",
            "BelAZ 7557", "KAMAZ-6520", "MAZ-5551", "KrAZ-65055", "URAL-43206",
            "BelAZ 75135", "KAMAZ-65802", "MAZ-6515", "KrAZ-65032", "URAL-55573",
            "ZIL 130", "ZIL 4331", "ZIL 164", "Shovel", "Spade", "Rake", "Hoe",
            "Tractor Belarus 820", "Tractor MTZ-1221", "Tractor Kirovets K-744", 
            "Tractor T-150K", "Tractor DT-75", "Tractor YUMZ-6", "Tractor KhTZ-181",
            "Tractor T-25", "Tractor T-40", "Tractor LTZ T-170", "Tractor DT-75M",
            "Tractor YUMZ-6K", "Tractor Belarus 1025", "Tractor MTZ-80", "Tractor T-16",
            "Tractor K-700A", "Tractor Belarus 892", "Tractor T-150", "Tractor MTZ-52",
            "Tractor T-70", "Combine Bizon Z056", "Combine Niva SK-5",
            "Combine Yenisei-1200", "Combine Don-1500", "Combine PALESSE GS12",
            "Combine Slobozhanets-20", "Combine Yenisei-1200A", "Combine PALESSE GS16",
            "Combine Dnieper-7", "Combine Niva SK-6", "Combine PALESSE GS575",
            "Combine Yenisei-1200M", "Combine PALESSE GS12R", "Combine Niva SK-3",
            "Combine Don-1500B", "Combine Yenisei-1200N", "Combine PALESSE GS4118",
            "Combine Niva SK-5M", "Combine Don-1500E", "Combine PALESSE GS4118K",
        )
        
        for device_name in data:
            self.cursor_obj.execute("INSERT INTO portable_devices (name) VALUES (%s)", (device_name,))
        
        con.commit()
        self.cursor_obj.close()
        con.close()
