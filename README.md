# Smart Farm Data Mocking Project

This project is designed to mock data for a smart farm application, populating the database with simulated data for various entities such as users, communities, plantings, harvests, cultivation devices, and more.

## Getting Started

To get started with this project, follow these instructions:

### Prerequisites

- Python 3.x
- PostgreSQL

### Installation

1. Clone this repository to your local machine:
    ```
    git clone https://github.com/administrative-dashboard/smart-farm.git
    cd smart-farm
    ```

2. Install the required Python dependencies using pip:
    ```
    pip install -r requirements.txt
    ```

3. Set up a PostgreSQL database and configure the connection details in the `config.py` file.

## PostgreSQL Database Setup

Follow these steps to set up your PostgreSQL database for the Smart Farm project:

1. **Log in to your PostgreSQL server:**

    ```
    sudo -u postgres psql
    ```

2. **Create a new user named `farm` with the password '123' and grant them the ability to create databases:**

    ```
    CREATE USER farm WITH PASSWORD '123';
    ALTER USER farm CREATEDB;
    ```

3. **Create a new database named `smart_farm`:**

    ```
    CREATE DATABASE smart_farm;
    ```

4. **Grant all privileges on the `smart_farm` database to the `farm` user:**

    ```sql
    GRANT ALL PRIVILEGES ON DATABASE smart_farm TO farm;
    ```

5. **Connect to the `smart_farm` database:**

    ```sql
    \c smart_farm
    ```

## Usage

After setting up the project and running the main script, the database will be populated with simulated data. You can then use this data to test your smart farm application.

```
git clone https://github.com/Kaammoo/data-mocking.git
cd data-mocking
python3 script/main.py
```

## Project Structure

- `main.py`: Main script to run data mocking process.
- `DataMocking.py`: Class containing methods to populate the database with simulated data.
- `Models.py`: Class containing methods to model various entities.
- `config.py`: Configuration file containing database connection details and other settings.
- `schema.sql`: SQL schema file containing the database schema.
- `requirements.txt`: List of Python dependencies.

## Contributing

Contributions are welcome! If you have any suggestions or improvements, feel free to open an issue or create a pull request.
