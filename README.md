# User Events Processing using Analytics Pipeline

## Overview with Architecture

### Overview
The user-generated events processing with an analytics pipeline follows a comprehensive approach that includes data extraction, cleaning, and transformation using Apache Spark. The cleaned data is then loaded into PostgreSQL for batch processing. For real-time analytics, the data is recorded in the Kafka topic and consumed into MongoDB. Visualizations are created using Tableau, MongoDB Charts, and Apache Superset for insightful analytics.

The pipeline utilizes technologies such as Apache Spark for batch processing, Kafka for real-time streaming, MongoDB for real-time inserts and charts, AWS S3 for data lake storage, and PostgreSQL for relational data storage. This allows for efficient data processing and storage at scale, enabling data-driven decision-making and improved business outcomes.

Superset is used for creating dashboards with SQL queries to optimize performance, while Tableau provides advanced visualization capabilities with custom SQL queries. The real-time pipeline includes simulated event stream generation using Python, inserting messages into the MongoDB Cloud database, and writing a backlog of messages to AWS S3. MongoDB Charts enables real-time data visualization, allowing for quick insights and monitoring of data changes.

The batch pipeline involves extracting data from AWS S3 into PySpark DataFrame, applying schema using Spark, and loading cleaned data into PostgreSQL. Overall, this runbook overview highlights the technology stack and steps involved in processing and analyzing user-generated events data for valuable insights and data-driven decision-making.

### Architecture

![image](https://github.com/saturn279/user-events-analytics/assets/45988700/5b8da7b2-b217-4ab5-8bdc-55386bf35f9e)

**Staging**
- Data Extraction
  - Reading the dataset from CSV files into PySpark DataFrames.
- Data Cleaning and Transformation
  - Applying schema and transformations using Apache Spark.
  - Generating tables like 'brand details' using grouping and aggregation on PySpark DataFrames.
- Data Loading
  - Cleaned DataFrames are loaded into PostgreSQL using PySpark and JD4BC connector.
- Visualization
  - Tableau dashboard (Postgres Connector and Custom SQL connector) with KPIs such as CVR, Cart Abandonment Rate, Repeat Customer Rate.

**Real-Time Pipeline**
- Publishers
  - Simulated event stream generation script (Python).
- Subscribers
  - MongoDB sink – inserts Kafka messages into MongoDB Cloud database with milliseconds delay.
  - AWS S3 sink – writes backlog of Kafka messages after every 1000 records to S3 in JSON format partitioned by the timestamp.
- Visualization
  - Real-time analytics dashboard with MongoDB Charts.

**Batch Pipeline**
- Data Extraction
  - Data of the selected time frame is read from S3 into PySpark DataFrame.
- Data Cleaning and Transformation
  - Apply schema using Spark.
- Data Loading
  - Cleaned DataFrames are loaded into PostgreSQL using PySpark and JDBC connector.

## Scope

**Data Extraction:**
- Read the e-commerce user events dataset from CSV files into PySpark DataFrames.
- Extract data of the selected time frame from AWS S3 into PySpark DataFrame for batch processing.

**Data Cleaning and Transformation:**
- Apply schema and perform necessary transformations using Apache Spark.
- Generate tables like 'brand details' using grouping and aggregation on PySpark DataFrames.

**Data Loading:**
- Load cleaned DataFrames into PostgreSQL using PySpark and JDBC connector for batch processing.
- Insert Kafka messages into MongoDB Cloud database with milliseconds delay for real-time processing.
- Write backlog of Kafka messages to AWS S3 in JSON format partitioned by timestamp for real-time processing.

**Visualization:**
- Create a Tableau dashboard with Postgres Connector and Custom SQL connector for visualizing KPIs such as CVR, Cart Abandonment Rate, Repeat Customer Rate.
- Create a real-time analytics dashboard with MongoDB Charts for visualizing real-time data.

## Pre-requisites

Make sure you have the following tools and services set up before running the pipeline:

- Confluent Cloud
- MongoDB Cloud
- Python 3
- Docker
- PySpark
- Jupyter Notebook
- PostgreSQL Server

## Screenshots
Here are some screenshots showcasing different aspects of the analytics pipeline:

### Apache Superset
![Apache Superset](Docs/Screenshots/Apache%20Superset.png)

### Confluent Recorded Events
![Confluent Recorded Events](Docs/Screenshots/Confluent%20Recorded%20Events.png)

### Confluent Sinks
![Confluent Sinks](Docs/Screenshots/Confluent%20Sinks.PNG)

### Kafka Simulated Stream Generator
![Kafka Simulated Stream Generator](Docs/Screenshots/Kafka%20simualated%20stream%20generator.png)

### MongoDB Documents
![MongoDB Documents](Docs/Screenshots/MongoDB%20Documents.png)

### MongoDB Real-time Charts
![MongoDB Real-time Charts](Docs/Screenshots/MongoDB%20Real-time%20Charts.png)

### MongoDB Traffic
![MongoDB Traffic](Docs/Screenshots/MongoDB%20Traffic.png)

### Postgres Row Count
![Postgres Row Count](Docs/Screenshots/Postgres%20Row%20Count.png)

### S3 Saved Events
![S3 Saved Events](Docs/Screenshots/S3%20saved%20events.PNG)


## References

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [MongoDB Charts Documentation](https://docs.mongodb.com/charts/)
- [Tableau Documentation](https://help.tableau.com/current/pro/desktop/en-us/dashboards.htm)
- [Superset Documentation](https://superset.apache.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [AWS S3 Documentation](https://aws.amazon.com/s3/)
