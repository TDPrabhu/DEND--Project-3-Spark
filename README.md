# DEND--Project-3-Spark
 ETL pipeline for a data lake hosted on S3

###  Introduction 
######  The Purpose of this Project is extracting the data from AWS S3 and ingest data into, which will be useful for generating the report based on user input. As the data is growing massively we are moving the infra to cloud. Redshift is designed with fact table and multiple dimension tables with required constraints on the column. Python script etl.py is used for extracting the data from the files provided, transform and store the data in the database. The script uses psycopg2 package to data munging and connect to the database. It does all the required validation like checking for the duplicate before inserting the data into the DB. Songplay Fact Table Users Dimension Table Songs Dimension Table Artists Dimension Table Time Dimension Table staging_songs Staging Table staging_events

### Table Creation
######  Tables are created by executing the Python script create_tables.py.
###### The create_tables.py in returns call the sql_queries.py which has all the DDL and DML statements in it.

### ETL Pipeline
######  Python etl.py is used to extract & transform the data from the file provided in S3 bucket and ingest the data into Redshift. It establishes the connection to the DB, then it extracts the required information from the files mentioned in the S3 bucket and copy the data into the appropriate tables. It checks for the duplicate before inserting the record into the tables. The code is modularized and provided all the comments. Most of the ETL is done with SQL statements.

### AWS Setup 
###### Need to create IAM role (‘myredshiftrole’) with permission to access the S3 bucket and redshift. Spin a new Redshift cluster and attach the role (‘myredshiftrole’) created to it, so that we can access the data from S3 and ingest in Redshift.

### Project Structure
###### create_tables.py - This script will drop the tables if exist and re-create new tables
###### etl.py - This script extract JSON data from the S3 bucket and executes the queries that ingest them to Redshift
###### sql_queries.py - This file contains variables with SQL statement in String formats, partitioned by CREATE, DROP, COPY and INSERT statements
###### dhw.cfg - Configuration file contains info about Redshift (Connection string and credentials), IAM and S3

### Steps to execute the code
###### 1. Open a new terminal
           First execute the create_tables.py.
           python create_tables.py
###### 2. Next Execute the etl.py
          python etl.py
