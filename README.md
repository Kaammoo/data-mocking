# Smart Farm Data Mocking Project

This project is designed to mock data for a smart farm application. It populates the database with simulated data for various entities such as users, communities, plantings, harvests, cultivation devices, and more.

## Getting Started

To get started with this project, follow these instructions:

### Prerequisites

- Python 3.x
- PostgreSQL

### Installation

1. Clone this repository to your local machine.
2. Install the required Python dependencies using pip:
    ```
    pip install -r requirements.txt
    ```
3. Set up a PostgreSQL database and configure the connection details in the `config.py` file.

## Usage

After setting up the project and running the main script, the database will be populated with simulated data. You can then use this data to test your smart farm application.

```
git clone https://github.com/Kaammoo/data-mocking.git
```

```
cd data-mocking
git checkout Develop
```

```
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

