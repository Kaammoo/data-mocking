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
        




    def insert_weather_metrics(self):
        self.cursor_obj.execute("""
            SELECT p.date, h.harvest_date 
            FROM planting p 
            INNER JOIN harvest h ON p.record_id = h.records_id
        """)
        sowing_harvest_dates = self.cursor_obj.fetchall()

        for sowing_date, harvest_date in sowing_harvest_dates:
            current_date = sowing_date
            while current_date < harvest_date:
                month = current_date.month
                current_season = get_season(month)

                if current_season == "Summer":
                    precipitation_types = ["rain"]
                    temp_range = (10, 40)
                    humidity_range = (30, 80)
                elif current_season == "Winter":
                    precipitation_types = ["snow", "hail"]
                    temp_range = (-10, 5)
                    humidity_range = (20, 70)
                else:
                    precipitation_types = ["rain", "snow", "hail"]
                    temp_range = (-5, 30)
                    humidity_range = (40, 70)

                humidity = random.randint(*humidity_range)
                temperature = random.randint(*temp_range)
                prec_type = random.choice(precipitation_types, None)
                if prec_type == "rain":
                    rain_drop = random.randint(10, 100)
                else:
                    rain_drop = None

                self.cursor_obj.execute("""
                    INSERT INTO weather_metrics (rain_drop, temperature, humidity, prec_type_id, date)
                    VALUES (%s, %s, %s, (SELECT id FROM prec_types WHERE name = %s), %s)
                """, (rain_drop, temperature, humidity, prec_type, current_date))

                current_date += datetime.timedelta(days=1)

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
        self.cursor_obj.execute("SELECT id FROM product_types WHERE type = 'cereal'")
        cereal_id = self.cursor_obj.fetchone()
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
    
    def insert_expense_categories(self):
        expense_categories = [
            ("fertilizer",), ("equipment costs",), ("seed purchase",), ("employee expenses",)
        ]
        for category in expense_categories:
            self.cursor_obj.execute("INSERT INTO expense_categories (name) VALUES (%s)", category)
            
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
