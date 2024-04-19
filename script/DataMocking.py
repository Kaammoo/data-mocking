import random
from faker import Faker
from db import con
from get_schema import fetch_schema
from configs import *
from consts import *
import datetime
import time 
from utilities import *
from config_handler import handle_config_changes
from Models import Models


class DataMocking:
    def __init__(self):
        self.cursor_obj = con.cursor()
        self.fake = Faker()
        self.schema = fetch_schema()
        self.models = Models(self)

        self.model_dependencies = {
            ("1", "plantings"): [
                self.models.model_users,
                self.models.model_product_types,
                self.models.model_products,
                self.models.model_measurement_units,
                self.models.model_fields,
                self.models.model_records,
                self.models.model_plantings,
                self.models.model_portable_devices,
                self.models.model_portable_devices_communities,
                self.models.model_planting_devices,
            ],
            ("2", "harvests"): [
                self.models.model_users,
                self.models.model_product_types,
                self.models.model_products,
                self.models.model_measurement_units,
                self.models.model_fields,
                self.models.model_records,
                self.models.model_plantings,
                self.models.model_portable_devices,
                self.models.model_portable_devices_communities,
                self.models.model_harvests,
                self.models.model_harvest_devices,
            ],
            ("3", "cultivations"): [
                self.models.model_users,
                self.models.model_product_types,
                self.models.model_products,
                self.models.model_measurement_units,
                self.models.model_precipitation_types,
                self.models.model_fields,
                self.models.model_records,
                self.models.model_plantings,
                self.models.model_portable_devices,
                self.models.model_portable_devices_communities,
                self.models.model_planting_devices,
                self.models.model_harvests,
                self.models.model_harvest_devices,
                self.models.model_weather_metrics,
                self.models.model_cultivations,
                self.models.model_expenses,
                self.update_calculate_yields,
                self.models.model_cultivation_devices,
                self.models.model_devices_calendars,
            ],
        }

    def insert_users(
        self,
        min_users_per_communitys=None,
        max_users_per_communitys=None,
        community_weights=None,
    ):
        start_time = time.time()
        # Fetch column names and data types for the users table from the schema
        user_columns = self.schema.get("users", {})
        if user_columns:
            column_names = ", ".join(
                [col_name for col_name in user_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(user_columns) - 1))

            # Provide default values if parameters are None
            min_users_per_community = int(min_users_per_communitys)
            max_users_per_community = int(max_users_per_communitys)

            # Fetch community names from the database
            self.cursor_obj.execute("SELECT name FROM communities")
            community_names = [entry[0] for entry in self.cursor_obj.fetchall()]

            users_data = []

            # Insert users into the users table
            for community_name in community_names:
                weight = community_weights.get(
                    community_name, 0.5
                )  # Default weight is 0.5 if not found
                adjusted_users = round(
                    ((max_users_per_community - min_users_per_community) * weight)
                    + min_users_per_community
                )

                for _ in range(adjusted_users):
                    name = self.fake.name()
                    unique_id = str(random.randint(1, 500))
                    email = self.fake.email() + f"_{unique_id}"
                    code = random.choice(
                        ["94", "98", "93", "33", "91", "99", "55", "95"]
                    )
                    phone_number = "+374" + code + self.fake.numerify("#######")

                    # Make sure the number of values matches the number of placeholders
                    users_data.append((name, email, phone_number, None))

            # Insert all users into the users table
            self.cursor_obj.executemany(
                f"INSERT INTO users ({column_names}) VALUES ({placeholders}) RETURNING id",
                users_data,
            )

            # Fetch all user IDs after inserting them
            self.cursor_obj.execute("SELECT id FROM users")
            user_ids = self.cursor_obj.fetchall()

            # Establish relationships between users and communities in the users_communities table
            users_communities_columns = self.schema.get("users_communities", {})
            if users_communities_columns:
                column_names = ", ".join(
                    [
                        col_name
                        for col_name in users_communities_columns.keys()
                        if col_name != "id"
                    ]
                )
                placeholders = ", ".join(["%s"] * (len(users_communities_columns) - 1))
            users_communities_data = []
            for community_name in community_names:
                community_id = community_names.index(community_name) + 1
                community_user_ids = random.sample(
                    user_ids,
                    k=round(len(user_ids) * community_weights.get(community_name, 0.5)),
                )
                for (user_id,) in community_user_ids:
                    users_communities_data.append((user_id, community_id))

            # Insert user-community relationships using executemany
            if users_communities_data:
                self.cursor_obj.executemany(
                    f"INSERT INTO users_communities ({column_names}) VALUES ({placeholders})",
                    users_communities_data,
                )
        end_time = time.time()  # End timing
        elapsed_time = end_time - start_time
        return elapsed_time, len(users_data)+len(users_communities_data)

    def insert_records(self, duration=None):
        start_time = time.time()
        # Fetch column names and data types for the records table from the schema
        record_columns = self.schema.get("records", {})
        if record_columns:
            column_names = ", ".join(
                [col_name for col_name in record_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(record_columns) - 1))

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
                records_data,
            )
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(records_data)

    def insert_expenses(self, duration=None):
        start_time = time.time()
        # Fetch column names and data types for the expenses table from the schema
        expenses_columns = self.schema.get("expenses", {})
        if expenses_columns:
            column_names = ", ".join(
                [col_name for col_name in expenses_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(expenses_columns) - 1))

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
            self.cursor_obj.executemany(
                f"""
                INSERT INTO expenses ({column_names})
                VALUES ({placeholders})
                """,
                insert_data,
            )
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(insert_data)

    def insert_portable_devices_communities(self, devices_weights=devices_weights):
        start_time = time.time()
        # Fetch column names and data types for the portable_devices_communities table from the schema
        pdc_columns = self.schema.get("portable_devices_communities", {})
        if pdc_columns:
            column_names = ", ".join(
                [col_name for col_name in pdc_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(pdc_columns) - 1))

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
                    input_device_count = get_input_device_count(
                        portable_device_name, devices_weights
                    )
                    if input_device_count > 0:
                        # Append tuple of values for each insertion
                        insertion_data.append(
                            (portable_device_id, community_id, input_device_count)
                        )

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO portable_devices_communities ({column_names}) VALUES ({placeholders})",
                insertion_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(insertion_data)

    def get_table_data_lenght(self, table_name, limit=10):
        self.cursor_obj.execute(f"SELECT COUNT(id) FROM {table_name} LIMIT {limit}")
        return self.cursor_obj.fetchone()[0]

    def insert_planting_devices(self):
        start_time = time.time()
        # Fetch column names and data types for the planting_devices table from the schema
        planting_devices_columns = self.schema.get("planting_devices", {})
        if planting_devices_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in planting_devices_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(planting_devices_columns) - 1))

            # Fetch planting and portable device community data
            self.cursor_obj.execute("SELECT id FROM plantings")
            plantings = self.cursor_obj.fetchall()
            self.cursor_obj.execute(
                "SELECT id, quantity FROM portable_devices_communities"
            )
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
                        insertion_data.append(
                            (planting_id, portable_device_community_id, insert_quantity)
                        )

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO planting_devices ({column_names}) VALUES ({placeholders})",
                insertion_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(insertion_data)

    def insert_harvest_devices(self):
        start_time = time.time()
        # Fetch column names and data types for the harvest_devices table from the schema
        harvest_devices_columns = self.schema.get("harvest_devices", {})
        if harvest_devices_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in harvest_devices_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(harvest_devices_columns) - 1))

            # Fetch harvests and portable device community data
            self.cursor_obj.execute("SELECT id FROM harvests")
            harvests = self.cursor_obj.fetchall()
            self.cursor_obj.execute(
                "SELECT id, quantity FROM portable_devices_communities"
            )
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
                        insertion_data.append(
                            (harvest_id, portable_device_community_id, insert_quantity)
                        )

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO harvest_devices ({column_names}) VALUES ({placeholders})",
                insertion_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(insertion_data)

    def update_calculate_yields(self):
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

            change = get_change_count(
                products_armenia, product_name, avg_fertilizer_quantity, avg_temp
            )

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
        start_time = time.time()
        # Fetch column names and data types for the weather_metrics table from the schema
        weather_columns = self.schema.get("weather_metrics", {})
        if weather_columns:
            column_names = ", ".join(
                [col_name for col_name in weather_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(weather_columns) - 1))

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

            # Fetch all precipitation types and their IDs from the database
            self.cursor_obj.execute("SELECT * FROM prec_types")
            prec_types = self.cursor_obj.fetchall()

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
                        precipitation_types, weights, temp_range, humidity_range = (
                            get_wether_date(current_season, month)
                        )
                        prec_type_name = random.choices(
                            precipitation_types, weights=weights
                        )[0]

                        # Find the ID of the precipitation type
                        prec_type_id = None
                        for row in prec_types:
                            if row[1] == prec_type_name:
                                prec_type_id = row[0]
                                break

                        humidity = random.randint(*humidity_range)
                        temperature = random.randint(*temp_range)

                        if prec_type_name == "rain":
                            rain_drop = random.randint(10, 100)
                        else:
                            rain_drop = None

                        if prec_type_id is not None:
                            weather_data.append(
                                (
                                    community_id,
                                    rain_drop,
                                    humidity,
                                    temperature,
                                    prec_type_id,
                                    current_date,
                                )
                            )

                        current_date += datetime.timedelta(days=1)

            # Insert weather data using executemany
            if weather_data:
                self.cursor_obj.executemany(
                    f"""
                    INSERT INTO weather_metrics ({column_names})
                    VALUES ({placeholders})
                    """,
                    weather_data,
                )
                end_time = time.time()  # End timing
                elapsed_time = end_time - start_time
                return elapsed_time, len(weather_data)

    def insert_product_types(self):
        start_time = time.time()
        # Fetch column names and data types for the product_types table from the schema
        product_type_columns = self.schema.get("product_types", {})
        if product_type_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in product_type_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(product_type_columns) - 1))

            product_type_data = [("vegetables",), ("cereal",)]

            self.cursor_obj.executemany(
                f"INSERT INTO product_types ({column_names}) VALUES ({placeholders})",
                product_type_data,
            )
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(product_type_data)

    def insert_products(self):
        start_time = time.time()
        # Fetch column names and data types for the products table from the schema
        product_columns = self.schema.get("products", {})
        if product_columns:
            column_names = ", ".join(
                [col_name for col_name in product_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(product_columns) - 1))

            self.cursor_obj.execute(
                "SELECT id FROM product_types WHERE type = 'vegetables'"
            )
            vegetable_id = self.cursor_obj.fetchone()[0]  # Fetching only the ID

            # List to store tuples of values to insert
            insertion_data = []

            for product in products_armenia:
                if "vegetable" in product[1].lower():
                    type_id = vegetable_id
                else:
                    type_id = vegetable_id+1
                print(type_id)

                # Append tuple of values for each product
                insertion_data.append((type_id, product[0], product[1]))

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO products ({column_names}) VALUES ({placeholders})",
                insertion_data,
            )
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(insertion_data)

    def insert_measurement_units(self):
        start_time = time.time()
        # Fetch column names and data types for the measurement_units table from the schema
        measurement_columns = self.schema.get("measurement_units", {})
        if measurement_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in measurement_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(measurement_columns) - 1))

            measurement_data = [(value, type) for value, type in measurements]

            # Insert measurement units into the measurement_units table
            self.cursor_obj.executemany(
                f"INSERT INTO measurement_units ({column_names}) VALUES ({placeholders})",
                measurement_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(measurement_data)

    def insert_revenues(self):
        start_time = time.time()
        # Fetch column names and data types for the revenues table from the schema
        revenue_columns = self.schema.get("revenues", {})
        if revenue_columns:
            column_names = ", ".join(
                [col_name for col_name in revenue_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(revenue_columns) - 1))

            self.cursor_obj.execute("SELECT id, yield, date FROM harvests")
            harvests = self.cursor_obj.fetchall()

            revenue_data = []
            for harvest_id, yield_amount, harvest_date in harvests:
                amount = round(yield_amount * random.uniform(10, 50), 2)
                revenue_date = harvest_date + datetime.timedelta(
                    days=random.randint(5, 10)
                )
                revenue_data.append((harvest_id, amount, revenue_date))

            # Insert revenues into the revenues table
            self.cursor_obj.executemany(
                f"INSERT INTO revenues ({column_names}) VALUES ({placeholders})",
                revenue_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(revenue_data)

    def insert_precipitation_types(self):
        start_time = time.time()
        # Fetch column names and data types for the prec_types table from the schema
        prec_types_columns = self.schema.get("prec_types", {})
        if prec_types_columns:
            column_names = ", ".join(
                [col_name for col_name in prec_types_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(prec_types_columns) - 1))

            # Define the precipitation types to insert
            precipitation_types = [("rain",), ("snow",), ("hail",), ("without_prec",)]

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO prec_types ({column_names}) VALUES ({placeholders})",
                precipitation_types,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(precipitation_types)

    def insert_expense_categories(self):
        start_time = time.time()
        # Fetch column names and data types for the expense_categories table from the schema
        expense_categories_columns = self.schema.get("expense_categories", {})
        if expense_categories_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in expense_categories_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(expense_categories_columns) - 1))

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO expense_categories ({column_names}) VALUES ({placeholders})",
                expense_categories,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(expense_categories)

    def insert_fields(
        self,
        min_field_count=min_field_count,
        max_field_count=max_field_count,
        min_field_size=min_field_size,
        max_field_size=max_field_size,
        field_owner_weights=field_owner_weights,
    ):
        start_time = time.time()
        # Fetch column names and data types for the fields table from the schema
        field_columns = self.schema.get("fields", {})
        if field_columns:
            column_names = ", ".join(
                [col_name for col_name in field_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(field_columns) - 1))

            # List to store tuples for bulk insertion
            fields_data = []

            self.cursor_obj.execute("SELECT * FROM communities")
            communities = self.cursor_obj.fetchall()
            self.cursor_obj.execute(
                "SELECT id FROM measurement_units WHERE value LIKE %s",
                ("square kilometres",),
            )
            measurement_unit_id = self.cursor_obj.fetchone()[0]

            # Iterate over communities to insert fields based on field ownership weights
            for community in communities:
                community_name = community[1]
                present_users_count = random.randint(min_field_count, max_field_count)
                max_users_count = round(
                    present_users_count * field_owner_weights.get(community_name, 0)
                )
                users_with_fields = random.randint(
                    1, max_users_count if max_users_count >= 1 else 1
                )  # Randomly select number of users with fields
                community_field_count = random.randint(
                    min_field_count,
                    max(min_field_count, min(max_field_count, max_users_count)),
                )  # Randomly select field count for community
                for user_index in range(users_with_fields):
                    if user_index < community_field_count:
                        field_name = f"{community_name}_field{user_index + 1}"
                        field_size = random.randint(min_field_size, max_field_size)
                        # Append tuple to fields_data list
                        fields_data.append(
                            (field_size, measurement_unit_id, field_name, None, None)
                        )
            # Perform bulk insertion
            self.cursor_obj.executemany(
                f"INSERT INTO fields ({column_names}) VALUES ({placeholders})",
                fields_data,
            )
            

            # Fetch column names for the fields_communities table from the schema
            fields_communities_columns = self.schema.get("fields_communities", {})
            if fields_communities_columns:
                fc_column_names = ", ".join(
                    [
                        col_name
                        for col_name in fields_communities_columns.keys()
                        if col_name != "id"
                    ]
                )
                fc_placeholders = ", ".join(
                    ["%s"] * (len(fields_communities_columns) - 1)
                )

                # List to store tuples for bulk insertion into fields_communities
                fields_communities_data = []

                for community in communities:
                    community_name = community[1]
                    present_users_count = random.randint(
                        min_field_count, max_field_count
                    )
                    max_users_count = round(
                        present_users_count
                        * field_owner_weights.get(community_name, 0.5)
                    )
                    users_with_fields = random.randint(
                        1, max_users_count
                    )  # Randomly select number of users with fields
                    if max_field_count >= min_field_count:
                        community_field_count = random.randint(
                            min(min_field_count, max_field_count),
                            max(max_field_count, min_field_count),
                        )

                    else:
                        community_field_count = min_field_count

                    for user_index in range(users_with_fields):
                        if user_index < community_field_count:
                            # Append tuple to fields_communities_data list
                            fields_communities_data.append(
                                (fields_data[user_index][0], community[0])
                            )

                # Perform bulk insertion into fields_communities
                self.cursor_obj.executemany(
                    f"INSERT INTO fields_communities ({fc_column_names}) VALUES ({fc_placeholders})",
                    fields_communities_data,
                )
                start_time = time.time()
                end_time = time.time()  # End timing
                elapsed_time = end_time - start_time
                return elapsed_time, len(fields_data)+len(fields_communities_data)

    def insert_portable_devices(self):
        start_time = time.time()
        # Fetch column names and data types for the portable_devices table from the schema
        portable_devices_columns = self.schema.get("portable_devices", {})
        if portable_devices_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in portable_devices_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(portable_devices_columns) - 1))

            # List to store tuples of values to insert
            insertion_data = []

            # Iterate over the portable device data and prepare insertion data
            for _, devices in portable_device_data.items():
                for device_name in devices:
                    insertion_data.append((device_name,))

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO portable_devices ({column_names}) VALUES ({placeholders})",
                insertion_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(insertion_data)

    def insert_cultivations(self):
        start_time = time.time()
        # Fetch column names and data types for the cultivations table from the schema
        cultivation_columns = self.schema.get("cultivations", {})
        if cultivation_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in cultivation_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(cultivation_columns) - 1))

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

                cultivation_data.append(
                    (
                        record_id,
                        start_date,
                        end_date,
                        avg_humidity,
                        avg_temp,
                        fertilizer_quantity,
                        water_amount,
                        workers_quantity,
                        other_factors,
                        irrigation_hours,
                        fertilizing_hours,
                        soil_compaction_hours,
                    )
                )
            self.cursor_obj.executemany(
                f"INSERT INTO cultivations ({column_names}) VALUES ({placeholders})",
                cultivation_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(cultivation_data)

    def insert_plantings(self, duration=None):
        start_time = time.time()
        # Fetch column names and data types for the plantings table from the schema
        planting_columns = self.schema.get("plantings", {})
        if planting_columns:
            column_names = ", ".join(
                [col_name for col_name in planting_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(planting_columns) - 1))

            self.cursor_obj.execute("SELECT id, product_id, field_id FROM records")
            records = self.cursor_obj.fetchall()
            used_records = {}
            years_duration = tuple((i for i in range(1, duration + 1)))

            # Prepare a list to store all tuples to be inserted
            insert_data = []

            for record in records:
                field_id = record[2]
                if field_id in used_records.keys():
                    if len(used_records[field_id]) >= duration:
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
                self.cursor_obj.execute(
                    f"SELECT name FROM products WHERE id = {record[1]}"
                )
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
                self.cursor_obj.execute(
                    f"SELECT size FROM fields WHERE id = {field_id}"
                )
                field_size = self.cursor_obj.fetchone()[0]
                workers_count = field_size // 10
                crop_count = get_crop_count(
                    field_size, min_crop_count, max_crop_count, workers_count
                )

                # Append the tuple to the list
                insert_data.append(
                    (record_id, crop_count, random_date_generated, workers_count)
                )
            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO plantings ({column_names}) VALUES ({placeholders})",
                insert_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(insert_data)

    def insert_harvests(self):
        start_time = time.time()
        # Fetch column names and data types for the harvests table from the schema
        harvest_columns = self.schema.get("harvests", {})
        if harvest_columns:
            column_names = ", ".join(
                [col_name for col_name in harvest_columns.keys() if col_name != "id"]
            )
            placeholders = ", ".join(["%s"] * (len(harvest_columns) - 1))

            # Fetch data from the plantings, records, and products tables
            self.cursor_obj.execute(
                "SELECT record_id, crop_quantity, date FROM plantings"
            )
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
                                harvest_date = planting_date + datetime.timedelta(
                                    days=rand_day
                                )

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
                                insert_data.append(
                                    (
                                        record_id,
                                        product_yield,
                                        harvest_date,
                                        field_size,
                                        workers_count,
                                    )
                                )

            # Execute the insert query using executemany
            self.cursor_obj.executemany(
                f"INSERT INTO harvests ({column_names}) VALUES ({placeholders})",
                insert_data,
            )
            start_time = time.time()
            end_time = time.time()  # End timing
            elapsed_time = end_time - start_time
            return elapsed_time, len(insert_data)

    def insert_cultivation_devices(self):
        start_time = time.time()
        # Fetch column names and data types for the cultivation_devices table from the schema
        cultivation_devices_columns = self.schema.get("cultivation_devices", {})
        if cultivation_devices_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in cultivation_devices_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(cultivation_devices_columns) - 1))

            # Fetch cultivations and portable devices communities
            self.cursor_obj.execute("SELECT id FROM cultivations")
            cultivations = self.cursor_obj.fetchall()
            self.cursor_obj.execute(
                "SELECT id, quantity FROM portable_devices_communities"
            )
            portable_devices_communities = self.cursor_obj.fetchall()

            cultivation_devices_data = []
            # Iterate over cultivations and portable devices communities to insert cultivation devices
            for cultivation in cultivations:
                cultivation_id = cultivation[0]
                for portable_device_community in portable_devices_communities:
                    portable_device_community_id = portable_device_community[0]
                    portable_device_community_quantity = portable_device_community[1]
                    insert_quantity = get_insert_quantity(
                        portable_device_community_quantity
                    )
                    if insert_quantity > 0:
                        cultivation_devices_data.append(
                            (
                                cultivation_id,
                                portable_device_community_id,
                                insert_quantity,
                            )
                        )

            # Insert cultivation devices into the cultivation_devices table using executemany
            if cultivation_devices_data:
                self.cursor_obj.executemany(
                    f"INSERT INTO cultivation_devices ({column_names}) VALUES ({placeholders})",
                    cultivation_devices_data,
                )
                start_time = time.time()
                end_time = time.time()  # End timing
                elapsed_time = end_time - start_time
                return elapsed_time, len(cultivation_devices_data)

    def insert_devices_calendars(self):
        start_time = time.time()
        # Fetch column names and data types for the devices_calendars table from the schema
        devices_calendars_columns = self.schema.get("devices_calendars", {})
        if devices_calendars_columns:
            column_names = ", ".join(
                [
                    col_name
                    for col_name in devices_calendars_columns.keys()
                    if col_name != "id"
                ]
            )
            placeholders = ", ".join(["%s"] * (len(devices_calendars_columns) - 1))
            # Fetch records, cultivations, harvests, and plantings data
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
            # Process plantings data
            planting_data = []
            for planting in plantings:
                planting_record_id, planting_start_date, planting_id = planting
                for record in records:
                    record_id, record_field_id = record
                    if record_id == planting_record_id:
                        self.cursor_obj.execute(
                            f"SELECT portable_devices_communities_id, quantity FROM planting_devices WHERE planting_id = {planting_id}"
                        )
                        planting_devices = self.cursor_obj.fetchall()
                        for (
                            portable_device_community_id,
                            portable_device_quantity,
                        ) in planting_devices:
                            planning_end_date = (
                                planting_start_date
                                + datetime.timedelta(days=random.randint(-2, 2))
                            )
                            planting_data.append(
                                (
                                    planting_start_date,
                                    planning_end_date,
                                    planting_start_date,
                                    portable_device_community_id,
                                    record_field_id,
                                    portable_device_quantity,
                                )
                            )
            # Process cultivations data
            cultivation_data = []
            for cultivation in cultivations:
                (
                    cultivation_record_id,
                    cultivation_start_date,
                    cultivation_end_date,
                    cultivation_id,
                ) = cultivation
                for record in records:
                    record_id, record_field_id = record
                    if record_id == cultivation_record_id:
                        self.cursor_obj.execute(
                            f"SELECT portable_devices_communities_id, quantity FROM cultivation_devices WHERE cultivation_id = {cultivation_id}"
                        )
                        cultivation_devices = self.cursor_obj.fetchall()
                        planning_end_date = cultivation_end_date + datetime.timedelta(
                            days=random.randint(-2, 2)
                        )
                        for (
                            portable_devices_communities_id,
                            portable_device_quantity,
                        ) in cultivation_devices:
                            cultivation_data.append(
                                (
                                    cultivation_start_date,
                                    planning_end_date,
                                    cultivation_end_date,
                                    portable_devices_communities_id,
                                    record_field_id,
                                    portable_device_quantity,
                                )
                            )
            # Process harvests data
            harvest_data = []
            for harvest in harvests:
                harvest_date, harvest_record_id, harvest_id = harvest
                for record in records:
                    record_id, record_field_id = record
                    if record_id == harvest_record_id:
                        self.cursor_obj.execute(
                            f"SELECT portable_devices_communities_id, quantity FROM harvest_devices WHERE harvest_id = {harvest_id}"
                        )
                        harvest_devices = self.cursor_obj.fetchall()
                        for (
                            portable_devices_communities_id,
                            portable_device_quantity,
                        ) in harvest_devices:
                            planning_end_date = harvest_date + datetime.timedelta(
                                days=random.randint(-2, 2)
                            )
                            harvest_data.append(
                                (
                                    harvest_date,
                                    planning_end_date,
                                    harvest_date,
                                    portable_devices_communities_id,
                                    record_field_id,
                                    portable_device_quantity,
                                )
                            )
            # Insert data into the devices_calendars table
            if planting_data or cultivation_data or harvest_data:
                self.cursor_obj.executemany(
                    f"""
                    INSERT INTO devices_calendars ({column_names})
                    VALUES ({placeholders})
                    """,
                    planting_data + cultivation_data + harvest_data,
                )
                start_time = time.time()
                end_time = time.time()  # End timing
                elapsed_time = end_time - start_time
                return elapsed_time, len(planting_data)+len(cultivation_data)+len(harvest_data)

    def insert_model(self, model_name, **args):
        for key in self.model_dependencies:
            if model_name in key:
                for func in self.model_dependencies[key]:
                    func()
                break  # Call each function associated with the model name

    def run(self):

        changes, models = handle_config_changes()
        for model in models:
            self.insert_model(model, **changes)
        con.commit()
        self.cursor_obj.close()
        con.close()
        
