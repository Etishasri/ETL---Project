# `ETL Project: Python and MySQL Integration`

This project demonstrates an **ETL (Extract, Transform, Load)** pipeline where data is extracted from a CSV file using Python, loaded into a MySQL database, and then cleaned and queried using SQL.

## 📁 Project Overview
The project is designed to showcase a complete ETL pipeline:

1. **Extract**: Load raw data from a CSV file using Python.
2. **Load**: Transfer the raw data into a MySQL database.
3. **Transform and Query**: Clean and preprocess the data directly in MySQL and perform SQL queries for analysis.

---

## 🛠️ Tools and Technologies

- **Python**: Used for data extraction and loading (libraries: `pandas`, `mysql.connector`).
- **MySQL**: Used for data cleaning, transformation, and querying.
- **CSV**: The raw data source.

---

## 📄 Steps in the ETL Pipeline

### 1. **Extract**
- Loaded the raw dataset from a CSV file using Python's `pandas` library.
- File path: `data/sample_data.csv`.

### 2. **Load**
- Connected to a MySQL database using `mysql.connector`.
- Created a table schema in MySQL for the dataset.
- Inserted the raw data into the database.

### 3. **Transform and Query**
- Performed data cleaning directly in MySQL, which included:
  - Handling missing values with `UPDATE` and `CASE` statements.
  - Standardizing data formats using SQL functions (e.g., `TRIM`, `UPPER`).
  - Removing duplicates with `DELETE` and `DISTINCT`.
- Queried the cleaned data to derive insights and perform analysis.

---

## 🔍 SQL Queries

### Query 1: DATA CONVERSION AS WELL AS DATA TYPE CONVERSION
```sql
create table netflix_stg as
with cte as (
select *, row_number() over (partition by title, types order by show_id) as rn
from netflix_data )
select show_id, types, title, str_to_date(date_added, '%M %d, %Y') as date_added, release_year, rating, case when duration is null then rating else duration end as duration, description
from cte;
select * from netflix_stg;
```

### Query 2:  MISSING VALUES for country and duration columns
```sql
insert into netflix_country
select show_id, m.country
from netflix_data nd
inner join (
select director, country 
from netflix_country nc
inner join netflix_director nf on nc.show_id = nf.show_id
group by director, country
) m on nd.director = m.director
where nd.country is null;
```

### Query 3: To check how many rows my data contains.
```sql
select count(*) from netflix_data;
```

### Query 4: check duplicates 
```sql
select show_id, count(*)
from netflix_data
group by show_id
having count(*) > 1;
```

---

## 📁 Project Structure

```plaintext
etl-project/
├── data/
│   └── sample_data.csv        # Raw data file
├── notebooks/
│   └── load_to_mysql.ipynb    # Python script to load data
├── sql/
│   ├── create_tables.sql      # MySQL table schema
│   └── queries.sql            # Analysis queries
```

---

## 🚀 How to Run

### Prerequisites
1. Install Python and MySQL.
2. Install the required Python libraries.
3. Set up a MySQL database and update the connection details in the Python script.

### Steps
1. Place the raw CSV file in the `data/` folder.
2. Run the Python script (`notebooks/load_to_mysql.ipynb`) to load the data into MySQL.
3. Use the provided SQL scripts in the `sql/` folder to clean and query the data.

---
