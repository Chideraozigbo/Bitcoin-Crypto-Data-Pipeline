# Bitcoin Crypto ETL Data Pipeline

## Project Overview

This project is an ETL (Extract, Transform, Load) pipeline built using Apache Airflow. It extracts cryptocurrency data from the CoinMarketCap API(specifically for Bitcoin), transforms the data using Pandas, and loads it into a PostgreSQL database.GitHub Actions is used for automatic deployment on the Ubuntu server.

## Key Components

### 1. Data Extraction

- **Source**: CoinMarketCap API
- **Endpoint**: `https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest`
- **Parameters**: Bitcoin (`'slug': 'bitcoin'`), converted to USD (`'convert': 'USD'`)
- **Headers**: Requires an API key for authentication

### 2. Data Transformation

- **Normalization**: Converts JSON response into a Pandas DataFrame
- **Cleaning**: Removes unnecessary columns
- **Formatting**: Converts numeric columns to appropriate types and formats
- **Renaming**: Updates column names for better readability
- **Date Conversion**: Converts date columns to datetime format

### 3. Data Loading

- **Primary Target**: PostgreSQL database
- **Fallback Target**: CSV file in case of database load failure

## DAG Structure

- **DAG Name**: `crypto_dag`
- **Schedule Interval**: `0 0 1 * *` (runs on the first day of every month)
- **Start Date**: June 21, 2024

### Tasks

1. **Extract Data Task**
   - Task ID: `extract_data_task`
   - Python Callable: `extract`
   
2. **Transform Data Task**
   - Task ID: `transform_data_task`
   - Python Callable: `transform`
   
3. **Load Data Task**
   - Task ID: `load_data_task`
   - Python Callable: `load`

## Error Handling

- In case of an exception during the data load into PostgreSQL, the data is saved as a CSV file with a timestamped filename.

## GitHub Actions

The repository includes a GitHub Actions workflow that automates the deployment to the Airflow server.

## Contributors
- [Chideraozigbo](https://github.com/Chideraozigbo)
- [Olawoyin007(Mentor)](https://github.com/Olawoyin007)

## Use Case and Importance

This ETL pipeline is essential for collecting and processing cryptocurrency data, specifically for Bitcoin. It automates the extraction, transformation, and loading processes, ensuring that the data is always up-to-date and accurately stored. This data can be used for various analytical and reporting purposes, providing valuable insights into the cryptocurrency market.
