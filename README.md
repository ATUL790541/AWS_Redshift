# AWS USE CASE

![Picture1](https://github.com/ATUL790541/AWS_Redshift/assets/75777816/0dc92b63-b077-4cd5-808c-99fa2461db66)


# Dataset Description

### Actives Dataset
| Column Names | Data Type |Transformations | Partition Columns|
|--------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|advertising_id| string | None|No|
|city |string|None|No|
|location_category |string|None|No|
|location_granularities|string|None|No|
|location_source |array[string]|Convert to a comma-separated string|No|
|state| string|None|No|
|timestamp|bigint|None|No|
|user_id |string|None|No|
|user_latitude| double|convert to decimal with 7 precision|No|
|user_ longitude|double|convert to decimal with 7 precision|No|
|month |string|None|Yes|
|date |date|None|Yes|


# Project Outline
 
 ### Created S3 bucket,dataset and config file
     -> Create a dataset using faker library in python by understanding the schema required.
     -> Create two s3 buckets namely landing zone and staging zone.
     -> Create a config file for spark describing the spark configuration and another one for paths called app config.
 ### Created EMR cluster and wrote spark job
     -> Create an EMR cluster using spark config file with a notebook attached to it.
     -> Write a spark job in such a way that it performs the transformation mentioned.
     -> The spark job should take the data set in the landing zone and after performing transformation sends the dataset to the staging zone.
     -> All the inputs based on path will be given to the spark job using the app config file.
### Created a EC2 instance and submitted spark job on airflow dag using LIVY 
     -> Create an EC2 instance and install airflow.
     -> Create a DAG such that it reads the config file and schedules the spark job submission using LIVY.
### Created Redshift cluster and copied data from S3 to Redshift using lambda
     -> 1.Create a Redshift serverless cluster. 
     -> Connect to the database using the Redshift query editor.
     -> Create Actives table with schema match.
     -> Write a lambda function that will be triggered when data reaches the Staging zone.
     -> Add Lambda Layer for psycopg2 and associate IAM Role.
     -> This lambda function should copy data from S3 to Redshift.

# Possible Extensions
     -> Data Security. Identify PII information and means to encrypt/secure this data 
     -> Data Ingestion from RDBMS, file systems. Can be an enhancement to this
     -> Performance Optimization-different join types for high volume data handling, partitioning, etc.1M++ records
     -> Logging – How to enable CloudWatch logs for different applications, how to monitor etc
     -> Getting comfortable with Unix commands and some basic scripting is important
     -> Basic CloudOps – IAM, permissions, role, etc. This can be a demo session
     
# References
  
  https://airflow.apache.org/docs/apache-airflow/stable/configurations-ref.html
  
  https://docs.aws.amazon.com/lambda/latest/dg/API_Reference.html
  
  https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/get-set-up-for-amazon-ec2.html
  
  https://www.geeksforgeeks.org/how-to-create-first-dag-in-airflow/
  
