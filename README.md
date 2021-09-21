# Data warehouse with AWS Redshift

## Introduction

This project is part of the Data Engineering Nanodegree by Udacity.  
It is an introduction to warehousing and ETL using AWS reshift.  

The purpose of this project is to build an ETL pipeline for a database hosted on Redshift, using data loaded from S3 into staging tables on Redshift. 
It uses data from logs and songs of a fictitious music streaming app called Sparkify.  
(links to the data can be found in the dwh.cfg file)

The purpose of this process would be to be able to conduct analytics on this data. Some example queries of what this could look like can be found in the example_analytics_queries.sql

## Files in the repository

- **create_dwh.ipynb** is a jupyter notebook with the different steps needed to create the redhift cluster needed for the project as well as the IAM role needed for it read S3 data and accept incoming traffic. Steps on how to use it are described below
- **sql_queries.py** contains the sql statements needed to create all the tables, copy data from S3, insert that data in the staging tables and transform and insert it into into the final star schema
- **dwh.cfg**  contains the AWS credentials needed to connect to the Udacity instance, the parameters for the Redshift cluster, its S3 IAM role arn and the links to the files on S3
- **create_tables.py** contains the script to connect to the warehouse using the credentials in dwh.cfg and create the tables using the queries in sql_queries
- **etl.py** contains the script load the staging tables and transform their data into the tables of the star schema
- **example_analytics_queries** contains simple examples of analytics queries

## Steps to run the project

1. Connect to AWS localy by inputing your access key and session token in your AWS config file
2. Create an admin user with programmatic access to connect to the AWS instance programmatically and save the access key in the AWS section of the dwh.cfg file
3. Donwload all necessary libraries (boto3, psycopg2) 
4. Run the different steps of the create_dwh.ipynb, input the IAM arn in the dwh.cfg file along the way
5. Run create_tables.py
6. Run etl.py
7. Clean you ressources using the last 2 cells of the jupyter notebook

