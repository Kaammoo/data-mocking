import random
from faker import Faker
from Db import con
from GetSchema import fetch_schema
from Configs import *
from Consts import *
import datetime
from time import time
from Utilities import *


class DataMocking:
    def __init__(self):
        self.cursor_obj = con.cursor()
        self.fake = Faker()
        self.schema = fetch_schema()

    def insert_users(self, min_users_per_communitys=min_users_per_community1, max_users_per_communitys=max_users_per_community1):
        # Fetch column names and data types for the users table from the schema
        user_columns = self.schema.get('users', {})
        if user_columns:
            column_names = ', '.join([col_name for col_name in user_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(user_columns) - 1))
            print(len(column_names))

            # Provide default values if parameters are None
            min_users_per_community = int(min_users_per_communitys)
            max_users_per_community = int(max_users_per_communitys)

            # Fetch community names from the database
            self.cursor_obj.execute("SELECT name FROM communities")
            community_names = [entry[0] for entry in self.cursor_obj.fetchall()]

            users_data = []

            # Insert users into the users table
            for community_name in community_names:
                weight = community_weights.get(community_name, 0.5)  # Default weight is 0.5 if not found
                adjusted_users = round(((max_users_per_community - min_users_per_community) * weight) + min_users_per_community)

                for _ in range(adjusted_users):
                    name = self.fake.name()
                    unique_id = str(random.randint(1, 500))
                    email = self.fake.email() + f"_{unique_id}"
                    code = random.choice(["94", "98", "93", "33", "91", "99", "55", "95"])
                    phone_number = "+374" + code + self.fake.numerify("#######")

                    # Make sure the number of values matches the number of placeholders
                    users_data.append((name, email, phone_number, None))

            # Insert all users into the users table
            print(f"users, {column_names = }")
            self.cursor_obj.executemany(
                f"INSERT INTO users ({column_names}) VALUES ({placeholders}) RETURNING id",
                users_data
            )

            # Fetch all user IDs after inserting them
            self.cursor_obj.execute("SELECT id FROM users")
            user_ids = self.cursor_obj.fetchall()

            # Establish relationships between users and communities in the users_communities table
            users_communities_columns = self.schema.get('users_communities', {})
            if users_communities_columns:
                column_names = ', '.join([col_name for col_name in users_communities_columns.keys() if col_name != 'id'])
                placeholders = ', '.join(['%s'] * (len(users_communities_columns) - 1))
            users_communities_data = []
            for community_name in community_names:
                community_id = community_names.index(community_name) + 1
                community_user_ids = random.sample(user_ids, k=round(len(user_ids) * community_weights.get(community_name, 0.5)))
                for user_id, in community_user_ids:
                    users_communities_data.append((user_id, community_id))

            # Insert user-community relationships using executemany
            if users_communities_data:
                print(f"users_communities, {column_names = }")
                self.cursor_obj.executemany(
                    f"INSERT INTO users_communities ({column_names}) VALUES ({placeholders})",
                    users_communities_data
                )

    def insert_records(self):
        # Fetch column names and data types for the records table from the schema
        record_columns = self.schema.get('records', {})
        if record_columns:
            column_names = ', '.join([col_name for col_name in record_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(record_columns) - 1))

            # List to store tuples for bulk insertion
            records_data = []

            self.cursor_obj.execute("SELECT * FROM communities")
            communities = self.cursor_obj.fetchall()
            self.cursor_obj.execute("SELECT * FROM products")
            products = self.cursor_obj.fetchall()
            
            for community in communities:
                community_name = community[1] + "%"
                community_id = community[0]
                self.cursor_obj.execute(
                    f"SELECT * FROM fields WHERE name LIKE '{community_name}'"
                )
                fields = self.cursor_obj.fetchall()
                for field in fields:
                    unique_products = random.sample(products, duration)
                    for product in unique_products:
                        # Append tuple to records_data list
                        records_data.append((community_id, field[0], product[0]))
            # Perform bulk insertion
            self.cursor_obj.executemany(
                f"INSERT INTO records ({column_names}) VALUES ({placeholders})",
                records_data
            )

    def insert_expenses(self):
        # Fetch column names and data types for the expenses table from the schema
        expenses_columns = self.schema.get('expenses', {})
        if expenses_columns:
            column_names = ', '.join([col_name for col_name in expenses_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(expenses_columns) - 1))

            self.cursor_obj.execute(
                """
                SELECT p.date, h.date, p.record_id 
                FROM plantings p 
                INNER JOIN harvests h ON p.record_id = h.record_id
                """
            )

            sowing_harvest_record_dates = self.cursor_obj.fetchall()

            # Prepare a list to store all tuples to be inserted
            insert_data = []

            for sowing_date, harvest_date, record_id in sowing_harvest_record_dates:
                period = (harvest_date - sowing_date).days

                if period <= 0:
                    continue

                for _ in range(duration):
                    expense_date = sowing_date + datetime.timedelta(
                        days=random.randint(0, period)
                    )

                    category_id = random.randint(1, 4)
                    amount = get_amount(category_id)
                    

                    # Append the tuple to the list
                    insert_data.append((record_id, category_id, amount, expense_date))

            # Execute the insert query using executemany
            print(column_names)
            self.cursor_obj.executemany(
                f"""
                INSERT INTO expenses ({column_names})
                VALUES ({placeholders})
                """,
                insert_data
            )

    def insert_portable_devices_communities(self):
        # Fetch column names and data types for the portable_devices_communities table from the schema
        pdc_columns = self.schema.get('portable_devices_communities', {})
        if pdc_columns:
            column_names = ', '.join([col_name for col_name in pdc_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(pdc_columns) - 1))

            # Fetch communities and portable devices from the database
            self.cursor_obj.execute("SELECT id FROM communities")
            communities = self.cursor_obj.fetchall()
            self.cursor_obj.execute("SELECT id, name FROM portable_devices")
            portable_devices = self.cursor_obj.fetchall()

            # List to store tuples of values to insert
            insertion_data = []

            for community in communities:
                community_id = community[0]
                for portable_device in portable_devices:
                    portable_device_id = portable_device[0]
                    portable_device_name = portable_device[1]
                    input_device_count = get_input_device_count(portable_device_name, devices_weights)
                    if input_device_count > 0:
                        # Append tuple of values for each insertion
                        insertion_data.append((portable_device_id, community_id, input_device_count))

            # Execute the insert query using executemany
            print(f"portable_devices_communities, {column_names}")
            self.cursor_obj.executemany(
                f"INSERT INTO portable_devices_communities ({column_names}) VALUES ({placeholders})",
                insertion_data
            )


    def insert_planting_devices(self):
        # Fetch column names and data types for the planting_devices table from the schema
        planting_devices_columns = self.schema.get('planting_devices', {})
        if planting_devices_columns:
            column_names = ', '.join([col_name for col_name in planting_devices_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(planting_devices_columns) - 1))

            # Fetch planting and portable device community data
            self.cursor_obj.execute("SELECT id FROM plantings")
            plantings = self.cursor_obj.fetchall()
            self.cursor_obj.execute("SELECT id, quantity FROM portable_devices_communities")
            portable_devices_communities = self.cursor_obj.fetchall()

            # List to store tuples of values to insert
            insertion_data = []

            # Iterate over plantings and portable device communities to prepare insertion data
            for planting in plantings:
                planting_id = planting[0]
                for portable_device_community in portable_devices_communities:
                    portable_device_community_id = portable_device_community[0]
                    portable_device_community_quantity = portable_device_community[1]
                    if portable_device_community_quantity > 7:
                        insert_quantity = random.randint(
                            0, portable_device_community_quantity // 4
                        )
                    else:
                        insert_quantity = random.randint(
                            0, portable_device_community_quantity
                        )
                    if insert_quantity > 0:
                        insertion_data.append((planting_id, portable_device_community_id, insert_quantity))

            # Execute the insert query using executemany
            print(f"planting_devices, {column_names = }")
            self.cursor_obj.executemany(
                f"INSERT INTO planting_devices ({column_names}) VALUES ({placeholders})",
                insertion_data
            )


    def insert_harvests_devices(self):
        # Fetch column names and data types for the harvest_devices table from the schema
        harvest_devices_columns = self.schema.get('harvest_devices', {})
        if harvest_devices_columns:
            column_names = ', '.join([col_name for col_name in harvest_devices_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(harvest_devices_columns) - 1))

            # Fetch harvests and portable device community data
            self.cursor_obj.execute("SELECT id FROM harvests")
            harvests = self.cursor_obj.fetchall()
            self.cursor_obj.execute("SELECT id, quantity FROM portable_devices_communities")
            portable_devices_communities = self.cursor_obj.fetchall()

            # List to store tuples of values to insert
            insertion_data = []

            # Iterate over harvests and portable device communities to prepare insertion data
            for harvest in harvests:
                harvest_id = harvest[0]
                for portable_device_community in portable_devices_communities:
                    portable_device_community_id = portable_device_community[0]
                    portable_device_community_quantity = portable_device_community[1]
                    if portable_device_community_quantity > 7:
                        insert_quantity = random.randint(
                            0, portable_device_community_quantity // 4
                        )
                    else:
                        insert_quantity = random.randint(
                            0, portable_device_community_quantity
                        )
                    if insert_quantity > 0:
                        insertion_data.append((harvest_id, portable_device_community_id, insert_quantity))

            # Execute the insert query using executemany
            print(f"harvest_devices, {column_names = }")
            self.cursor_obj.executemany(
                f"INSERT INTO harvest_devices ({column_names}) VALUES ({placeholders})",
                insertion_data
            )


    def calculate_yields(self):
        self.cursor_obj.execute(
            """
            SELECT record_id, avg_temperature, other_factors, water_amount, fertilizer_quantity
            FROM cultivations
        """
        )
        cultivation_records = self.cursor_obj.fetchall()

        for record in cultivation_records:
            (
                record_id,
                avg_temp,
                other_factors,
                avg_water_amount,
                avg_fertilizer_quantity,
            ) = record

            self.cursor_obj.execute(
                """
                SELECT p.name
                FROM records r
                JOIN products p ON r.product_id = p.id
                WHERE r.id = %s
            """,
                (record_id,),
            )
            product_name = self.cursor_obj.fetchone()[0]

            self.cursor_obj.execute(
                """
                SELECT yield
                FROM harvests
                WHERE record_id = %s
            """,
                (record_id,),
            )
            old_yield = self.cursor_obj.fetchone()[0]

            change = get_change_count(products_armenia,product_name,avg_fertilizer_quantity,avg_temp)

            change += other_factors

            new_yield = old_yield - old_yield * change / 100

            self.cursor_obj.execute(
                """
                UPDATE harvests
                SET yield = %s
                WHERE record_id = %s
            """,
                (new_yield, record_id),
            )

    def insert_weather_metrics(self):
        # Fetch column names and data types for the weather_metrics table from the schema
        weather_columns = self.schema.get('weather_metrics', {})
        if weather_columns:
            column_names = ', '.join([col_name for col_name in weather_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(weather_columns) - 1))

            self.cursor_obj.execute(
                """
                SELECT p.date, r.community_id
                FROM plantings p
                INNER JOIN records r ON p.record_id = r.id
            """
            )
            sowing_dates_and_communities = self.cursor_obj.fetchall()
            self.cursor_obj.execute(
                """
                SELECT date 
                FROM harvests
            """
            )
            harvest_dates = self.cursor_obj.fetchall()

            # Prepare data for executemany
            weather_data = []

            for sowing_date, community_id in sowing_dates_and_communities:
                corresponding_harvest_date = None
                for (harvest_date,) in harvest_dates:
                    if harvest_date > sowing_date:
                        corresponding_harvest_date = harvest_date
                        break
                if corresponding_harvest_date:
                    current_date = sowing_date
                    while current_date < corresponding_harvest_date:
                        month = current_date.month
                        current_season = get_season(month)
                        precipitation_types, weights, temp_range, humidity_range = get_wether_date()
                        prec_type = random.choices(precipitation_types, weights=weights)[0]
                        humidity = random.randint(*humidity_range)
                        temperature = random.randint(*temp_range)
                        if prec_type == "rain":
                            rain_drop = random.randint(10, 100)
                        else:
                            rain_drop = None
                        if prec_type is not None:
                            weather_data.append((
                                community_id,
                                rain_drop,
                                humidity,
                                temperature,
                                prec_type,
                                current_date,
                            ))

                        current_date += datetime.timedelta(days=1)

            # Insert weather data using executemany
            if weather_data:
                print(f"weather_metrics, {column_names = }")
                self.cursor_obj.executemany(
                    f"""
                    INSERT INTO weather_metrics ({column_names})
                    VALUES ({placeholders})
                    """,
                    weather_data
                )

    def insert_product_types(self):
        # Fetch column names and data types for the product_types table from the schema
        product_type_columns = self.schema.get('product_types', {})
        if product_type_columns:
            column_names = ', '.join([col_name for col_name in product_type_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(product_type_columns) - 1))

            product_type_data = [("vegetables",), ("cereal",)]
            for product_type in product_type_data:
                sql = f"INSERT INTO product_types ({column_names}) VALUES ({placeholders})"
                self.cursor_obj.execute(sql, product_type)

    def insert_products(self):
        # Fetch column names and data types for the products table from the schema
        product_columns = self.schema.get('products', {})
        if product_columns:
            column_names = ', '.join([col_name for col_name in product_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(product_columns) - 1))

            self.cursor_obj.execute(
                "SELECT id FROM product_types WHERE type = 'vegetables'"
            )
            vegetable_id = self.cursor_obj.fetchone()[0]  # Fetching only the ID
            self.cursor_obj.execute("SELECT id FROM product_types WHERE type = 'cereal'")
            cereal_id = self.cursor_obj.fetchone()[0]  # Fetching only the ID

            # List to store tuples of values to insert
            insertion_data = []

            for product in products_armenia:
                if "vegetable" in product[1].lower():
                    type_id = vegetable_id
                else:
                    type_id = cereal_id

                # Append tuple of values for each product
                insertion_data.append((type_id, product[0], product[1]))

            # Execute the insert query using executemany
            print(f"products, {column_names = }")
            self.cursor_obj.executemany(
                f"INSERT INTO products ({column_names}) VALUES ({placeholders})",
                insertion_data
            )

    def insert_measurement_units(self):
        # Fetch column names and data types for the measurement_units table from the schema
        measurement_columns = self.schema.get('measurement_units', {})
        if measurement_columns:
            column_names = ', '.join([col_name for col_name in measurement_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(measurement_columns) - 1))

            measurement_data = [(value, type) for value, type in measurements]

            # Insert measurement units into the measurement_units table
            self.cursor_obj.executemany(
                f"INSERT INTO measurement_units ({column_names}) VALUES ({placeholders})",
                measurement_data
            )

    def insert_revenues(self):
        # Fetch column names and data types for the revenues table from the schema
        revenue_columns = self.schema.get('revenues', {})
        if revenue_columns:
            column_names = ', '.join([col_name for col_name in revenue_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(revenue_columns) - 1))

            self.cursor_obj.execute("SELECT id, yield, date FROM harvests")
            harvests = self.cursor_obj.fetchall()

            revenue_data = []
            for harvest_id, yield_amount, harvest_date in harvests:
                amount = round(yield_amount * random.uniform(10, 50), 2)
                revenue_date = harvest_date + datetime.timedelta(days=random.randint(5, 10))
                revenue_data.append((harvest_id, amount, revenue_date))

            # Insert revenues into the revenues table
            self.cursor_obj.executemany(
                f"INSERT INTO revenues ({column_names}) VALUES ({placeholders})",
                revenue_data
            )

    def insert_precipitation_types(self):
        # Fetch column names and data types for the prec_types table from the schema
        prec_types_columns = self.schema.get('prec_types', {})
        if prec_types_columns:
            column_names = ', '.join([col_name for col_name in prec_types_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(prec_types_columns) - 1))

            # Define the precipitation types to insert
            precipitation_types = [("rain",), ("snow",), ("hail",), ("without_prec",)]

            # Execute the insert query using executemany
            print(column_names)
            self.cursor_obj.executemany(
                f"INSERT INTO prec_types ({column_names}) VALUES ({placeholders})",
                precipitation_types
            )

    def insert_expense_categories(self):
        # Fetch column names and data types for the expense_categories table from the schema
        expense_categories_columns = self.schema.get('expense_categories', {})
        if expense_categories_columns:
            column_names = ', '.join([col_name for col_name in expense_categories_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(expense_categories_columns) - 1))

            # Define the expense categories to insert
            expense_categories = [
                ("fertilizer",),
                ("equipment costs",),
                ("seed purchase",),
                ("employee expenses",),
            ]
            print(column_names)
            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO expense_categories ({column_names}) VALUES ({placeholders})",
                expense_categories
            )

    def fields(self, min_field_count=min_field_count, max_field_count=max_field_count):
        # Fetch column names and data types for the fields table from the schema
        field_columns = self.schema.get('fields', {})
        if field_columns:
            column_names = ', '.join([col_name for col_name in field_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(field_columns) - 1))

            # List to store tuples for bulk insertion
            fields_data = []

            self.cursor_obj.execute("SELECT * FROM communities")
            communities = self.cursor_obj.fetchall()

            for community in communities:
                for i in range(random.randint(min_field_count, max_field_count)):
                    field_name = f"{community[1]}_field{i + 1}"
                    field_size = random.randint(min_field_size, max_field_size)
                    # Append tuple to fields_data list
                    fields_data.append((field_size, 4, field_name, None, None))
            # Perform bulk insertion
            self.cursor_obj.executemany(
                f"INSERT INTO fields ({column_names}) VALUES ({placeholders})",
                fields_data
            )

            # Fetch column names for the fields_communities table from the schema
            fields_communities_columns = self.schema.get('fields_communities', {})
            if fields_communities_columns:
                fc_column_names = ', '.join([col_name for col_name in fields_communities_columns.keys() if col_name != 'id'])
                fc_placeholders = ', '.join(['%s'] * (len(fields_communities_columns) - 1))

                # List to store tuples for bulk insertion into fields_communities
                fields_communities_data = []

                for community in communities:
                    community_name = community[1] + "%"
                    community_id = community[0]
                    self.cursor_obj.execute(
                        f"SELECT * FROM fields WHERE name LIKE '{community_name}'"
                    )
                    fields = self.cursor_obj.fetchall()
                    for field in fields:
                        # Append tuple to fields_communities_data list
                        fields_communities_data.append((field[0], community_id))

                # Perform bulk insertion into fields_communities
                self.cursor_obj.executemany(
                    f"INSERT INTO fields_communities ({fc_column_names}) VALUES ({fc_placeholders})",
                    fields_communities_data
                )

    def insert_portable_devices(self):
        # Fetch column names and data types for the portable_devices table from the schema
        portable_devices_columns = self.schema.get('portable_devices', {})
        if portable_devices_columns:
            column_names = ', '.join([col_name for col_name in portable_devices_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(portable_devices_columns) - 1))

            # List to store tuples of values to insert
            insertion_data = []

            # Iterate over the portable device data and prepare insertion data
            for _, devices in portable_device_data.items():
                for device_name in devices:
                    insertion_data.append((device_name,))

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO portable_devices ({column_names}) VALUES ({placeholders})",
                insertion_data
            )

    def insert_cultivations(self):
        # Fetch column names and data types for the cultivations table from the schema
        cultivation_columns = self.schema.get('cultivations', {})
        if cultivation_columns:
            column_names = ', '.join([col_name for col_name in cultivation_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(cultivation_columns) - 1))

            self.cursor_obj.execute(
                """
                SELECT p.date, h.date, p.record_id 
                FROM plantings p 
                INNER JOIN harvests h ON p.record_id = h.record_id
            """
            )
            sowing_harvest_record_dates = self.cursor_obj.fetchall()

            cultivation_data = []
            for sowing_date, harvest_date, record_id in sowing_harvest_record_dates:
                start_date = (
                    sowing_date + datetime.timedelta(days=random.randint(1, 2))
                ).strftime("%Y-%m-%d %H:%M:%S.%f %z")
                end_date = (
                    harvest_date - datetime.timedelta(days=random.randint(1, 2))
                ).strftime("%Y-%m-%d %H:%M:%S.%f %z")
                self.cursor_obj.execute(
                    """
                    SELECT community_id 
                    FROM records 
                    WHERE id = %s
                """,
                    (record_id,),
                )
                community_id = self.cursor_obj.fetchone()[0]
                query = f"""
                    SELECT AVG(humidity), AVG(temperature) 
                    FROM weather_metrics 
                    WHERE community_id = {community_id} AND date BETWEEN '{start_date}' AND '{end_date}'
                """
                self.cursor_obj.execute(query)
                avg_humidity, avg_temp = self.cursor_obj.fetchone()
                self.cursor_obj.execute(
                    """
                    SELECT AVG(rain_drop)
                    FROM weather_metrics
                    WHERE community_id = %s AND date BETWEEN %s AND %s
                """,
                    (community_id, start_date, end_date),
                )
                water_amount = self.cursor_obj.fetchone()[0]

                self.cursor_obj.execute(
                    """
                    SELECT AVG(workers_quantity)
                    FROM plantings
                    WHERE record_id = %s
                """,
                    (record_id,),
                )
                workers_quantity = self.cursor_obj.fetchone()[0]

                avg_humidity = round(avg_humidity, 2)
                avg_temp = round(avg_temp, 2)
                if water_amount is not None:
                    water_amount = round(water_amount, 2)
                else:
                    water_amount = 0

                self.cursor_obj.execute(
                    """
                    SELECT p.name
                    FROM records r
                    JOIN products p ON r.product_id = p.id
                    WHERE r.id = %s
                """,
                    (record_id,),
                )
                product_name = self.cursor_obj.fetchone()[0]

                other_factors = round(random.uniform(0.1, 0.49), 2)
                for product in products_armenia:
                    if product[0] == product_name:
                        min_count_fertilizer = product[12]
                        max_count_fertilizer = product[13]
                        fertilizer_quantity = round(
                            random.randint(min_count_fertilizer, max_count_fertilizer)
                        )
                        break
                irrigation_hours = random.randint(18, 25)
                fertilizing_hours = random.randint(18, 25)
                soil_compaction_hours = random.randint(18, 25)
                
                cultivation_data.append((
                    record_id,
                    start_date,
                    end_date,
                    avg_humidity,
                    avg_temp,
                    fertilizer_quantity,
                ))
            self.cursor_obj.executemany(
            f"INSERT INTO cultivations ({column_names}) VALUES ({placeholders})",
            cultivation_data
            )

    def insert_plantings(self, info_duration=duration):
        # Fetch column names and data types for the plantings table from the schema
        planting_columns = self.schema.get('plantings', {})
        if planting_columns:
            column_names = ', '.join([col_name for col_name in planting_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(planting_columns) - 1))

            self.cursor_obj.execute("SELECT id, product_id, field_id FROM records")
            records = self.cursor_obj.fetchall()
            used_records = {}
            years_duration = tuple((i for i in range(1, info_duration + 1)))

            # Prepare a list to store all tuples to be inserted
            insert_data = []

            for record in records:
                field_id = record[2]
                if field_id in used_records.keys():
                    if len(used_records[field_id]) >= info_duration:
                        continue
                    elif len(used_records[field_id]) == 1:
                        years_range = tuple(
                            x for x in years_duration if x != used_records[field_id][0]
                        )
                        rand_year = random.choice(years_range)
                        used_records[field_id].append(rand_year)
                    else:
                        years_range = tuple(
                            item
                            for item in years_duration
                            if item not in used_records[field_id]
                        )
                        rand_year = random.choice(years_range)
                        used_records[field_id].append(rand_year)
                else:
                    rand_year = random.choice(years_duration)
                    used_records[field_id] = [rand_year]
                self.cursor_obj.execute(f"SELECT name FROM products WHERE id = {record[1]}")
                product_name = self.cursor_obj.fetchone()[0]

                crop_info = get_crops_and_months_by_product_name(
                    products_armenia, product_name
                )

                if crop_info is None:
                    continue
                min_crop_count, max_crop_count, min_month, max_month = crop_info
                random_date_generated = random_date_within_months(
                    min_month, max_month, rand_year
                )
                record_id = record[0]
                self.cursor_obj.execute(f"SELECT size FROM fields WHERE id = {field_id}")
                field_size = self.cursor_obj.fetchone()[0]
                workers_count = field_size // 10
                crop_count = get_crop_count(field_size, min_crop_count, max_crop_count)

                # Append the tuple to the list
                insert_data.append((record_id, crop_count, random_date_generated, workers_count))
            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO plantings ({column_names}) VALUES ({placeholders})",
                insert_data
            )

    def insert_harvests(self):
        # Fetch column names and data types for the harvests table from the schema
        harvest_columns = self.schema.get('harvests', {})
        if harvest_columns:
            column_names = ', '.join([col_name for col_name in harvest_columns.keys() if col_name != 'id'])
            placeholders = ', '.join(['%s'] * (len(harvest_columns) - 1))

            # Fetch data from the plantings, records, and products tables
            self.cursor_obj.execute("SELECT record_id, crop_quantity, date FROM plantings")
            plantings_data = self.cursor_obj.fetchall()

            self.cursor_obj.execute("SELECT id, product_id, field_id FROM records")
            records = self.cursor_obj.fetchall()

            self.cursor_obj.execute("SELECT id, name FROM products")
            products = self.cursor_obj.fetchall()

            # Prepare a list to store all tuples to be inserted
            insert_data = []

            # Iterate over plantings data
            for planting in plantings_data:
                planting_record_id = planting[0]
                planting_date = planting[2]
                
                # Find the corresponding record and product information
                for record in records:
                    record_id = record[0]
                    if planting_record_id == record_id:
                        record_product_id = record[1]
                        
                        for product in products:
                            product_id = product[0]
                            if record_product_id == product_id:
                                product_name = product[1]
                                (
                                    min_growth_duration,
                                    max_growth_duration,
                                    min_yield,
                                    max_yield,
                                ) = get_growth_duration_and_min_max_yield_by_product_name(
                                    products_armenia, product_name
                                )
                                rand_day = random.randint(
                                    min_growth_duration, max_growth_duration
                                )
                                harvest_date = planting_date + datetime.timedelta(days=rand_day)

                                field_id = record[2]
                                self.cursor_obj.execute(
                                    f"SELECT size FROM fields WHERE id = {field_id}"
                                )
                                field_size = self.cursor_obj.fetchone()[0]
                                workers_count = field_size // 10
                                if workers_count > 9:
                                    workers_count -= random.randint(0, 2)
                                else:
                                    workers_count += random.randint(0, 2)
                                product_yield = field_size * random.randint(
                                    min_yield, max_yield
                                )
                                planting_date = planting_date.strftime(
                                    "%Y-%m-%d %H:%M:%S.%f %z"
                                )
                                harvest_date = harvest_date.strftime(
                                    "%Y-%m-%d %H:%M:%S.%f %z"
                                )
                                # Append the tuple to the list
                                insert_data.append((record_id, product_yield, harvest_date, field_size, workers_count))

            # Execute the insert query using executemany
            print(f"harvests, {column_names}")
            self.cursor_obj.executemany(
                f"INSERT INTO harvests ({column_names}) VALUES ({placeholders})",
                insert_data
            )


    def insert_cultivation_devices(self):
        self.cursor_obj.execute("SELECT id FROM cultivations")
        cultivations = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT id, quantity FROM portable_devices_communities")
        portable_devices_communities = self.cursor_obj.fetchall()
        for cultivation in cultivations:
            cultivation_id = cultivation[0]
            for portable_device_community in portable_devices_communities:
                portable_device_community_id = portable_device_community[0]
                portable_device_community_quantity = portable_device_community[1]
                insert_quantity = get_insert_quantity(portable_device_community_quantity)
                if insert_quantity > 0:
                    self.cursor_obj.execute(
                        "INSERT INTO cultivation_devices (cultivation_id, portable_devices_communities_id, quantity) VALUES (%s, %s, %s)",
                        (cultivation_id, portable_device_community_id, insert_quantity),
                    )

    def insert_devices_calendars(self):
        self.cursor_obj.execute("SELECT id, field_id FROM records")
        records = self.cursor_obj.fetchall()
        self.cursor_obj.execute(
            "SELECT record_id, start_date, end_date, id FROM cultivations"
        )
        cultivations = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT record_id, date, id FROM harvests")
        harvests = self.cursor_obj.fetchall()
        self.cursor_obj.execute("SELECT record_id, date, id FROM plantings")
        plantings = self.cursor_obj.fetchall()
        for planting in plantings:
            planting_record_id = planting[0]
            planting_start_date = planting[1]
            planting_id = planting[2]
            for record in records:
                record_id = record[0]
                if record_id == planting_record_id:
                    record_field_id = record[1]
                    self.cursor_obj.execute(
                        f"SELECT portable_devices_communities_id, quantity FROM planting_devices WHERE planting_id = {planting_id}"
                    )
                    planting_devices = self.cursor_obj.fetchall()
                    for cultivation in cultivations:
                        cultivation_record_id = cultivation[0]
                        if cultivation_record_id == record_id:
                            cultivation_start_date = cultivation[1]
                            planning_end_date = (
                                cultivation_start_date
                                + datetime.timedelta(days=random.randint(-2, 2))
                            )
                            for planting_device in planting_devices:
                                portable_device_community_id = planting_device[0]
                                portable_device_quantity = planting_device[1]
                                self.cursor_obj.execute(
                                    "INSERT INTO devices_calendars (start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity) VALUES (%s, %s, %s, %s, %s, %s)",
                                    (
                                        planting_start_date,
                                        planning_end_date,
                                        cultivation_start_date,
                                        portable_device_community_id,
                                        record_field_id,
                                        portable_device_quantity,
                                    ),
                                )
        for cultivation in cultivations:
            cultivation_record_id = cultivation[0]
            cultivation_start_date = cultivation[1]
            cultivation_end_date = cultivation[2]
            cultivation_id = cultivation[3]
            for record in records:
                record_id = record[0]
                if record_id == cultivation_record_id:
                    record_field_id = record[1]
                    self.cursor_obj.execute(
                        f"SELECT portable_devices_communities_id, quantity FROM cultivation_devices WHERE cultivation_id = {cultivation_id}"
                    )
                    cultivation_devices = self.cursor_obj.fetchall()
                    planning_end_date = cultivation_end_date + datetime.timedelta(
                        days=random.randint(-2, 2)
                    )
                    for cultivation_device in cultivation_devices:
                        portable_devices_communities_id = cultivation_device[0]
                        portable_device_quantity = cultivation_device[1]
                        self.cursor_obj.execute(
                            "INSERT INTO devices_calendars (start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity) VALUES (%s, %s, %s, %s, %s, %s)",
                            (
                                cultivation_start_date,
                                planning_end_date,
                                cultivation_end_date,
                                portable_devices_communities_id,
                                record_field_id,
                                portable_device_quantity,
                            ),
                        )

        for harvest in harvests:
            harvest_date = harvest[1]
            harvest_record_id = harvest[0]
            harvest_id = harvest[2]
            for record in records:
                record_id = record[0]
                if record_id == harvest_record_id:
                    record_field_id = record[1]
                    self.cursor_obj.execute(
                        f"SELECT portable_devices_communities_id, quantity FROM harvest_devices WHERE harvest_id = {harvest_id}"
                    )
                    harvest_devices = self.cursor_obj.fetchall()
                    for cultivation in cultivations:
                        cultivation_record_id = cultivation[0]
                        if cultivation_record_id == record_id:
                            cultivation_end_date = cultivation[2]
                            planning_end_date = harvest_date + datetime.timedelta(
                                days=random.randint(-2, 2)
                            )
                            for harvest_device in harvest_devices:
                                portable_device_communitie_id = harvest_device[0]
                                portable_device_quantity = harvest_device[1]
                                self.cursor_obj.execute(
                                    "INSERT INTO devices_calendars (start_date, end_date, actual_end_date, portable_devices_communities_id, field_id, quantity) VALUES (%s, %s, %s, %s, %s, %s)",
                                    (
                                        cultivation_end_date,
                                        planning_end_date,
                                        harvest_date,
                                        portable_device_communitie_id,
                                        record_field_id,
                                        portable_device_quantity,
                                    ),
                                )

    def insert_table(self, table_name, **args):
        if table_name == "1":
            if "min_users_per_community" in args:
                self.insert_users(args["min_users_per_community"])
                print("Users table inserted successfully")
            elif "max_users_per_community" in args:
                self.insert_users(args["max_users_per_community"])
                print("Users table inserted successfully")
            elif "max_users_per_community" in args and "min_users_per_community" in args:
                self.insert_users(args["max_users_per_community"])
                print("Users table inserted successfully") 
            else:
                self.insert_users()
                print("Users table inserted successfully")
        elif table_name == "2":
            self.insert_measurement_units()
            print("Measurement units table inserted successfully")
        elif table_name == "3":
            self.fields()
            print("Fields table inserted successfully")
        elif table_name == "4":
            self.insert_precipitation_types()
            print("Precipitation types table inserted successfully")
        elif table_name == "5":
            self.insert_product_types()
            print("Product types table inserted successfully")
        elif table_name == "6":
            self.insert_products()
            print("Products table inserted successfully")
        elif table_name == "7":
            self.insert_records()
            print("Records table inserted successfully")
        elif table_name == "8":
            self.insert_expense_categories()
            print("Expense categories table inserted successfully")
        elif table_name == "9":
            self.insert_portable_devices()
            print("Portable devices table inserted successfully")
        elif table_name == "10":
            self.insert_portable_devices_communities()
            print("Portable devices communities table inserted successfully")
        elif table_name == "11":
            self.insert_plantings()
            print("Plantings table inserted successfully")
        elif table_name == "12":
            self.insert_harvests()
            print("Harvest table inserted successfully")
        elif table_name == "13":
            self.insert_planting_devices()
            print("Planting devices table inserted successfully")
        elif table_name == "14":
            self.insert_harvests_devices()
            print("Harvest devices table inserted successfully")
        elif table_name == "15":
            self.insert_expenses()
            print("Expenses table inserted successfully")
        elif table_name == "16":
            self.insert_revenues()
            print("Revenues table inserted successfully")
        elif table_name == "17":
            self.insert_weather_metrics()
            print("Weather metrics table inserted successfully")
        elif table_name == "18":
            self.insert_cultivations()
            print("Cultivations table inserted successfully")
        elif table_name == "19":
            self.calculate_yields()
            print("Yields calculated successfully")
        elif table_name == "20":
            self.insert_cultivation_devices()
            print("Cultivation devices table inserted successfully")
        elif table_name == "21":
            self.insert_devices_calendars()
            print("Devices calendars table inserted successfully")
        else:
            print(f"Table number {table_name} is not recognized.")

    def run(self):
        change = input("Do you need to change anything? (y/yes or n/no): ")
        changes = {}
        if change.lower() in ["y", "yes"]:
            while True:
                change_input = input("Enter the change you want to make (key=new_value), or enter 'done' to finish: ")
                if change_input.lower() == "done":
                    break
                else:
                    key, value = change_input.split("=")
                    changes[key] = value

        if change.lower() in ["n", "no"]:
            yes = input("If you need to run all tables, press 'y'. If not, press 'n': ")

            if yes.lower() == "n":
                table_count = int(input("How many tables do you need to insert: "))
                if table_count >= 1:
                    print(
                        "Please specify the tables to insert: Press table numbers which you woth to insert splite its with space:"
                    )
                    tables()
                    table_names = input(
                        "Enter table numbers separated by space: "
                    ).split()
                    table_names = [int(name) for name in table_names]
                    table_names.sort()
                    table_names= [str(name) for name in table_names]
                    if table_count == table_names.__len__():
                        for table_name in table_names:
                            self.insert_table(table_name)
                    else:
                        print("Tble names much more than table count")
            elif yes.lower() == "y":
                for table_name in range(1, 22):
                    self.insert_table(str(table_name))
        elif change.lower() in ["y", "yes"]:
            for table_name in range(1, 22):
                self.insert_table(str(table_name), **changes)
        
        con.commit()
        self.cursor_obj.close()
        con.close()
            
