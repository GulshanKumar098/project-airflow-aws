# Data Pipelines with Apache Airflow

## 📘 Project Overview
The music streaming company **Sparkify** wants to automate and monitor its existing ETL process that populates their data warehouse in AWS Redshift.  
This project uses **Apache Airflow** to design a **dynamic, reusable, and monitored data pipeline** that stages data from S3, loads it into Redshift, and runs data quality checks to ensure accuracy and consistency.

---

## 🎯 Goal
To build a **production-grade data pipeline** using **Apache Airflow** that:
1. Extracts and stages data from **S3** into **Amazon Redshift**
2. Transforms and loads the data into **fact and dimension tables**
3. Validates the data with **automated data quality checks**

---

## 🏗️ Project Structure

project-airflow-aws/
│
├── dags/
│ ├── create_tables_dag.py
│ ├── etl_dag.py
│ ├── greet_flow_dag.py
│ ├── sql_statements.py
│ ├── test_list_s3_keys_by_airflow_dag.py
│ └── udac_example_dag.py
│
├── imgs/
│ ├── data_pipeline.png
│ ├── ER diagram - Udacity Project Data Warehouse.png
│ ├── log-data.png
│ └── log-json-path.png
│
├── plugins/
│ ├── helpers/
│ │ ├── init.py
│ │ └── sql_queries.py
│ │
│ └── operators/
│ ├── init.py
│ ├── data_quality.py
│ ├── load_dimension.py
│ ├── load_fact.py
│ └── stage_redshift.py
│
├── test/
│ └── ...
│
└── README.md

