# Order Processing with Apache Airflow and DataProc

## Overview
This project automates the processing of daily order files stored in a GCP bucket using Apache Airflow and Apache Spark. The workflow reads order files for a specific date, filters delivered orders, and stores the processed data in an output folder.

## Objective
The goal is to:
1. Process daily `orders_yyyymmdd.csv` files from the `/data` folder in GCP.
2. Handle cases where multiple files are present for a given date.
3. Filter only `Delivered` orders.
4. Store the processed data in the `/output` folder.
5. Automate the workflow using Apache Airflow and execute the Spark job on an existing DataProc cluster.

## Workflow Steps in Apache Airflow

### 1. Import Required Libraries
   - All necessary libraries for Apache Airflow and GCP interaction are imported.

### 2. Define Default Arguments
   - The `default_args` dictionary is declared to manage task retries and execution timeouts.

### 3. Initialize DAG Object
   - A DAG object is created using the `DAG()` function.
   - Airflow variables for `cluster_name`, `region`, and `project_id` are set, as the Spark job runs on an existing DataProc cluster.

### 4. Define Python Function `get_orders_date`
   - This function retrieves the execution date.
   - If no date is provided, it assigns today's date; otherwise, it returns the user-specified date.

### 5. Create Task to Get Execution Date
   - `PythonOperator` is used to execute `get_orders_date` and map its result using Airflow XCom.

### 6. Create Task to Submit Spark Job
   - `DataProcSubmitPySparkJobOperator()` is used to submit the Spark job.
   - The cluster name, project ID, and region are retrieved using Airflow variables.
   - The `--date` argument is passed from Airflow XCom.

### 7. Define Execution Order
   - The tasks are executed in the following order:
     ```
     get_execution_date_task >> submit_pyspark_job
     ```

## Technologies Used
- **Google Cloud Platform (GCP)**: For DataProc cluster and storage.
- **Apache Airflow**: For workflow orchestration.
- **Apache Spark**: For data processing.
- **Python**: For scripting DAG and Spark job.

## Execution Instructions
1. Deploy the Airflow DAG in your Airflow environment.
2. Ensure that necessary permissions are granted for Airflow to access GCP resources.
3. Trigger the DAG manually or schedule it to run daily.
4. Monitor the execution in the Airflow UI.

## Expected Output
- The Spark job processes the order data for the given date.
- Orders with `order_status == "Delivered"` are filtered.
- The filtered data is stored in the `/output` folder.

## Conclusion
This project automates order data processing using Airflow and DataProc, ensuring efficient handling of daily data files while maintaining flexibility for manual date selection.

