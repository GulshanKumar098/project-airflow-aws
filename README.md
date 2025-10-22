# **Sparkify Data Pipeline using Apache Airflow**

## **1. Project Overview**

The music streaming company **Sparkify** wants to enhance automation and visibility in their data warehouse ETL workflows.  
After evaluating several technologies, they selected **Apache Airflow** as the orchestration tool to manage and monitor their ETL processes effectively.

The goal of this project is to create **dynamic, modular, and reusable data pipelines** that:
- Contain **parameterized and reusable tasks**
- Allow **monitoring, scheduling, and backfills**
- Include **data quality checks** after ETL execution

### 🎯 **Objective**
Develop an **ETL data pipeline on AWS** using **Apache Airflow** that performs the following steps:
1. Load raw data from S3 into Redshift staging tables  
2. Transform and populate fact and dimension tables  
3. Run automated quality checks on the final data  

---

## **2. Project Data Sources**

The project uses datasets stored in **Amazon S3**.

### **2.1 Song Data**
Contains metadata for songs and artists.

**Example record:**
```json
{
  "num_songs": 1,
  "artist_id": "ARJIE2Y1187B994AB7",
  "artist_latitude": null,
  "artist_longitude": null,
  "artist_location": "",
  "artist_name": "Line Renaud",
  "song_id": "SOUPIRU12A6D4FA1E1",
  "title": "Der Kleine Dompfaff",
  "duration": 152.92036,
  "year": 0
}
```

### **2.2 Log Data**

Represents user activity logs on the Sparkify platform, such as which user listened to which song, the time of play, and the device used.

A JSON metadata file —\*\*s3://udacity-dend/log\_json\_path.json\*\* —is provided to correctly map fields when using Redshift’s **COPY** command to load data.

**3\. Data Warehouse Schema Design**
------------------------------------

The Redshift warehouse is designed with **staging tables**, a **fact table**, and several **dimension tables** to support analytical queries efficiently.

### **3.1 Staging Tables**

Temporary tables used to hold raw data before transformation:

*   staging\_events
    
*   staging\_songs
    

### **3.2 Fact Table**

*   **songplays** — contains records of song play events (filtered by page = 'NextSong')
    

### **3.3 Dimension Tables**

*   **users** — user details from the app
    
*   **songs** — song metadata
    
*   **artists** — artist details
    
*   **time** — timestamps of songplays broken into units (hour, day, week, month, etc.)
    

**4\. Airflow Data Pipeline**
-----------------------------

The ETL workflow is defined in:

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   ./dags/etl_dag.py   `

### **4.1 How to Run the Pipeline**

1.  Open the **Airflow Web UI**
    
2.  Enable the DAG named **etl**
    
3.  Trigger a manual run or let it execute as per the schedule
    

The DAG includes multiple **custom operators**, each responsible for a specific ETL stage.

### **4.2 Stage Operator**

**File:** ./plugins/operators/stage\_redshift.py

This operator loads raw data from **S3 to Amazon Redshift** using the COPY command.It supports both **CSV** and **JSON** data formats.

**Functions:**

*   Stage\_events → loads log data into staging\_events
    
*   Stage\_songs → loads song data into staging\_songs
    

**Features:**

*   Configurable S3 path, table name, JSON path, and IAM credentials
    
*   Reusable for multiple datasets
    

### **4.3 Fact Table Load Operator**

**File:** ./plugins/operators/load\_fact.py

Loads transformed data into the **fact table (songplays)**.Fact tables generally contain a large volume of data, so this operator performs **append-only inserts**.

**Function:**

*   Load\_songplays\_fact\_table → inserts data into songplays
    

### **4.4 Dimension Table Load Operator**

**File:** ./plugins/operators/load\_dimension.py

Populates dimension tables from staging data.Supports two data load modes:

*   **Append mode** → adds new rows
    
*   **Truncate-insert mode** → clears and reloads all data
    

**Functions:**

*   Load\_user\_dim\_table
    
*   Load\_song\_dim\_table
    
*   Load\_artist\_dim\_table
    
*   Load\_time\_dim\_table
    

### **4.5 Data Quality Operator**

**File:** ./plugins/operators/data\_quality.py

This operator performs post-load validation checks to ensure data accuracy and completeness.

**Quality Checks Include:**

*   Ensuring target tables are **not empty**
    
*   Verifying that **NOT NULL** columns have valid values
    

**Function:**

*   Run\_data\_quality\_checks → executes validation tests on all final tables
    

**5\. Project Workflow Summary**
--------------------------------

This project demonstrates how to design and implement a **robust ETL pipeline** using **Apache Airflow** and **Amazon Redshift**.The pipeline ensures:

*   Data extraction from S3 into Redshift staging tables
    
*   Transformation and loading into analytics-ready schemas
    
*   Automated data validation after each pipeline run
    

The structure is modular, maintainable, and optimized for data reliability and scalability.

**6\. Technologies Used**
-------------------------

*   **Apache Airflow** — Workflow orchestration
    
*   **Amazon Redshift** — Data warehousing
    
*   **Amazon S3** — Cloud data storage
    
*   **Python** — Script development and operator logic
    
*   **SQL** — Data transformation queries
    

**7\. Author**
--------------

**Name:** Gulshan

**Project:** Sparkify Data Warehouse Automation using Apache Airflow