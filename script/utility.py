import random
from faker import Faker
from db import con
from consts import *
from datetime import datetime, timedelta
import time


class DataMocking:
    def __init__(self):
        self.cursor_obj = con.cursor()
        self.fake = Faker()

    def insert_users(self, users_per_community):
        self.cursor_obj.execute("SELECT id FROM communities")
        communities = self.cursor_obj.fetchall()[0]
        unique_id = str(int(time.time()))
        for community_id in range(1,11):
            for _ in range(users_per_community):
                name = self.fake.name()
                email = self.fake.email() + f"_{unique_id}"
                code = random.choice(["94", "98", "93", "33", "91", "99", "55", "95"])
                phone_number = "+374" + code + self.fake.numerify('#######')

                self.cursor_obj.execute("INSERT INTO users (name, email, phone_number) VALUES (%s, %s, %s) RETURNING id",
                                        (name, email, phone_number))
        self.cursor_obj.execute("SELECT * FROM communities")
        communities = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT * FROM users")
        users = self.cursor_obj.fetchall()
        for community in communities:
            community_id = community[0]
            for user in users:
                user_id = user[0]
                
                self.cursor_obj.execute("INSERT INTO users_communities (user_id, community_id) VALUES (%s, %s)",
                                        (user_id, community_id))

        con.commit() 
        self.cursor_obj.close()
        con.close()
    
    def insert_product_type(self):
        self.cursor_obj.execute("SELECT id FROM products")
        product_type_data = [
            ("vegetables",),
            ("cereal",)
        ]
        for product_type in product_type_data:
            sql = "INSERT INTO product_types (type) VALUES (%s)"
            self.cursor_obj.execute(sql, product_type)
            
        con.commit()
        self.cursor_obj.close()
        con.close()

    def insert_products(self):
        self.cursor_obj.execute("SELECT id FROM product_types WHERE type = 'vegetables'")
        vegetable_id = self.cursor_obj.fetchone()
        # vegetable_type_id = vegetable_type[0] if vegetable_type else None

        self.cursor_obj.execute("SELECT id FROM product_types WHERE type = 'cereal'")
        cereal_id = self.cursor_obj.fetchone()
        # cereal_type_id = cereal_type[0] if cereal_type else None

        for product in products_armenia:
            if "vegetable" in product[1].lower():
                type_id = vegetable_id
            else:
                type_id = cereal_id
            
            self.cursor_obj.execute("INSERT INTO products (type_id, name, description) VALUES (%s, %s, %s)",\
                (type_id, product[0], product[1]))

        con.commit()
        self.cursor_obj.close()
        con.close()


    def insert_measurement_units(self):
        measurements = [
            ("liter", "volume"),
            ("meter", "length"),
            ("Celsius", "temperature"),
            ("g/m^3", "density"),
        ]
        for measurement in measurements:
            self.cursor_obj.execute("INSERT INTO measurement_units (value, type) VALUES (%s,%s)", (measurement[0], measurement[1]))
        con.commit()
        self.cursor_obj.close()
        con.close()



    def insert_records(self):
        self.cursor_obj.execute("SELECT * FROM communities")
        communities = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT * FROM products")
        products = self.cursor_obj.fetchall()
        for community in communities:
            community_name = community[1] + "%"
            community_id = community[0]
            self.cursor_obj.execute(f"SELECT * FROM fields WHERE name LIKE '{community_name}'")
            fields = self.cursor_obj.fetchall()
            for field in fields:
                unique_products = random.sample(products, 5)
                for product in unique_products:
                    self.cursor_obj.execute("INSERT INTO records (community_id, field_id, product_id) VALUES (%s, %s, %s)", \
                                        (community_id, field[0], product[0]))
        con.commit()
        self.cursor_obj.close()
        con.close()

            
    def insert_precipitation_types(self):
        precipitation_types = [
            ("rain", ),
            ("snow",),
            ("hail",)
        ]
        
        for prec_type in precipitation_types:
            self.cursor_obj.execute("INSERT INTO prec_types (name) VALUES (%s)", prec_type)
        
        con.commit()
        self.cursor_obj.close()
        con.close()
    

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
        pass
