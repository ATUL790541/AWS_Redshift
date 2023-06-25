# IMPORT DEPENDENCIES
from datetime import datetime
import requests, boto3, botocore, json, time
from botocore.exceptions import ClientError
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
import pyarrow.parquet as pq
import s3fs
import pandas as pd
import numpy as np
from decimal import Decimal


# USER DEFINED PARAMETERS
S3_BUCKET_NAME = 'rising-cubs-landing-bucket'
S3_STAGING_BUCKET='rising-cubs-staging-bucket'
APP_CONFIG = 'config_file/app_config.json'
SPARK_CONFIG = 'config_file/spark_config.json'
LAMBDA_TRIGGER='lambda_trigger/lambda_trigger.txt'
SENDER = 'sneha.rajan@tigeranalytics.com'
RECIPIENT = 'sneha.rajan@tigeranalytics.com'
SUBJECT = 'Airflow DAG Failure Notification'
KEY_NAME = 'Rishing_Cubs_kp'
REGION = 'ap-southeast-1'
SECURITY_GROUP_NAME = 'RisingCubs'
# EMR PARAMETERS 
INSTANCE_TYPE = 'm5.xlarge'
MASTER_NODE = 1
WORKER_NODE = 2


# EMAIL NOTIFICATION ON FAILURE
def send_email_notification(context):
    """
    This function sends an email notification when a task in the DAG fails.
    """
    task_instance = context.get('task_instance')
    task_name = task_instance.task_id
    execution_date = task_instance.execution_date
    dag_id = task_instance.dag_id
    message = f"DAG {dag_id} task {task_name} failed at {execution_date}."
    client = boto3.client('ses', region_name=REGION)
    try:
        response = client.send_email(
            Destination={
                'ToAddresses': [RECIPIENT]
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': 'UTF-8',
                        'Data': message,
                    },
                },
                'Subject': {
                    'Charset': 'UTF-8',
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
        )
    except ClientError as e:
        print(f"Failed to send email: {e}")
        raise AirflowException(f"Failed to send email: {e}")
    print(f"Email sent to {RECIPIENT}")


# FETCH SUBNET ID
def get_subnet_id(REGION):
    """
    Gets the ID of a subnet.
    :return: The ID of the subnet.
    """
    ec2 = boto3.client('ec2', region_name=REGION)
    try:
        response = ec2.describe_subnets()
    except ClientError as e:
        print(f"Failed to describe subnets: {e}")
        return None
    if len(response['Subnets']) > 1:
        return response['Subnets'][1]['SubnetId']
    print("No subnets found")
    return None   


# FETCH SECURITY ID
def get_security_group_id(REGION, SECURITY_GROUP_NAME):
    """
    Gets the ID of a security group.

    :return: The ID of the security group.
    """

    try:
        ec2 = boto3.client('ec2', region_name=REGION)
        response_security_group = ec2.describe_security_groups()
        for sg in response_security_group['SecurityGroups']:
            if sg['GroupName'] == SECURITY_GROUP_NAME:
                return sg['GroupId']
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
        

# READ JSON FILE FROM S3 BUCKET
def get_files(bucket_name,file_name):
    """
    Reads a JSON file from an S3 bucket.

    :param bucket_name: The name of the S3 bucket.
    :param key: The key of the JSON file in the S3 bucket.
    :return: A dictionary representing the JSON file.
    """

    s3 = boto3.client('s3')
    obj=s3.get_object(Bucket=bucket_name,Key=file_name)
    contents=obj['Body'].read().decode('utf-8')
    try:
        file_json = json.loads(contents)
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON file: {e}")
        return None
    
    return file_json

#Reads the app config file 
def read_file_from_s3(bucket_name=S3_BUCKET_NAME, key=APP_CONFIG):
    """
    Reads file from get_files functon
    
    params bucket_name: enter the bucket name to be accessed
    params key: enter the file path inside the bucket name to be accessed
    return json file
    """
    try:
        file_json=get_files(S3_BUCKET_NAME,APP_CONFIG)
        print(file_json)
    except ClientError as e:
        print(f"Failed to read file from S3: {e}")
        return None
    return file_json

             
# CLUSTER CREATION
def create_cluster(S3_BUCKET_NAME, SPARK_CONFIG, REGION, KEY_NAME, INSTANCE_TYPE, MASTER_NODE, WORKER_NODE):
    """
    Creates EMR cluster with specified configuration.

    :params S3_BUCKET_NAME: bucket containg spark configuration file
    :params SPARK_CONFIG: spark config path
    :params REGION: region of interest in AWS
    :params KEY_NAME: ppk file to ssh into EMR
    :return: job_flow_id and master_public_dns
    """
    # Configuration parameters for EMR cluster
    try:
        spark_config = get_files(S3_BUCKET_NAME, SPARK_CONFIG)
        # Subnet id
        SUBNET_ID = get_subnet_id(REGION)
        if SUBNET_ID is None:
            raise ValueError("Subnet ID is None. Cannot create EMR cluster.")
        # Security id
        SECURITY_ID = get_security_group_id(REGION, SECURITY_GROUP_NAME)
        if SECURITY_ID is None:
            raise ValueError("Security ID is None. Cannot create EMR cluster.")
        # Defining emr cluster
        client = boto3.client('emr', region_name=REGION)
        # Bootstrap
        bootstrap_action = {
                'Name': 'Install Boto3',
                'ScriptBootstrapAction': {
                    'Path': 's3://rising-cubs-landing-bucket/Dependencies/dependencies.sh'
                    }
                }
        # Instances to create
        instance_groups = [
            {
                'Name': "Master",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': INSTANCE_TYPE,
                'InstanceCount': MASTER_NODE,
            },
            {
                'Name': "Workers",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': INSTANCE_TYPE,
                'InstanceCount': WORKER_NODE,
            }
        ]
        # Configuring cluster
        cluster_id = client.run_job_flow(
            Name="cubs-cluster",
            Instances={
                'InstanceGroups': instance_groups,
                'EmrManagedMasterSecurityGroup':SECURITY_ID,
                'EmrManagedSlaveSecurityGroup':SECURITY_ID,
                'Ec2KeyName': KEY_NAME,
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'Ec2SubnetId': SUBNET_ID,
            },
            LogUri="s3://rising-cubs-landing-bucket/emr_logs/",
            ReleaseLabel= 'emr-5.36.0',
            BootstrapActions=[bootstrap_action],
            VisibleToAllUsers=True,
            JobFlowRole="EMR_EC2_DefaultRole",
            ServiceRole="EMR_DefaultRole",
            Configurations=spark_config,
            Applications = [ {'Name': 'Spark'},{'Name':'Hadoop'},{'Name':'JupyterHub'},{'Name':'Hive'},{'Name':'JupyterEnterpriseGateway'},{'Name':'Livy'}],
            AutoTerminationPolicy={'IdleTimeout':600}
        )
        # Wait for emr cluster to change its state to waiting
        waiter = client.get_waiter('cluster_running')
        waiter.wait(ClusterId=cluster_id['JobFlowId'])
        # Extract JobFlowId and MasterPublicDnsName
        cluster = client.describe_cluster(ClusterId=cluster_id['JobFlowId'])
        master_public_dns = cluster['Cluster']['MasterPublicDnsName']
        print("EMR cluster created, job flow id is:")
        print(cluster_id['JobFlowId'])
        print("Master Public DNS:")
        print(master_public_dns)
        return cluster_id['JobFlowId'], master_public_dns
    except (ValueError, botocore.exceptions.ClientError) as e:
        print(f"Error creating EMR cluster: {e}")
        return None, None
    except Exception as e:
        print(f"Unknown error: {e}")
        return None, None

    
# SUBMIT SPARK JOB USING LIVY REST API
def submit_spark_job(ti,REGION,S3_STAGING_BUCKET,LAMBDA_TRIGGER):
    """
    Submit the spark job via Livy rest api and terminates the EMR cluster
    """
    # parameters needed
    try:
        config = ti.xcom_pull(task_ids='read_file_from_s3')
        SPARK_JOB_KEY = config["transformation-dataset"]['etl_job_path']
        print(str(SPARK_JOB_KEY[0]))
        DATASET = config["transformation-dataset"]['datasets']
        print(str(DATASET[0]))
        SOURCE_LOCATION = config["transformation-dataset"]['source']['data-location']
        print(str(SOURCE_LOCATION))
    except:
        raise AirflowException(f"Failed to fetch xcom data from 'read_file_from_s3' task: {e}")
        
    # read spark job file
    try:
        SPARK_JOB_KEY = {'file':SPARK_JOB_KEY}
        print("Spark job file read.")
    except:
        print(f"No Spark job file found on the location")

    # to access livy we need cluster dns and jobFlowId
    try:
        job_flow_id, master_public_dns = ti.xcom_pull(task_ids='create_cluster')
    except Exception as e:
        raise AirflowException(f"Failed to fetch xcom data from 'create_cluster' task: {e}")


    # access livy to submit spark job
    try:
        livy_url = 'http://' + master_public_dns + ':8998/batches/'
        # post request
        headers = {'Content-Type': 'application/json'}
        data={"file":"s3://rising-cubs-landing-bucket/spark_job/rising_cubs_etl_job.py","className":"com.example.SparkApp","args":["Actives.parquet","s3://rising-cubs-landing-bucket/dataset/"]}
        r = requests.post(livy_url, data=json.dumps(data), headers=headers)
        r.raise_for_status()  # raise an exception if the status code indicates an error
        # wait for livy job to complete
        livy_job_id = r.json()['id']
    except requests.exceptions.RequestException as e:
        print(f"Error submitting Spark job to Livy: {e}")
        # handle the exception 
    else:
        while True:
            time.sleep(10)
            try:
                r = requests.get(livy_url + str(livy_job_id), headers={'Content-Type': 'application/json'})
                r.raise_for_status()  # raise an exception if the status code indicates an error
                status = r.json()['state']
                if status in ['success', 'dead', 'killed', 'failed']:
                    break
            except requests.exceptions.RequestException as e:
                print(f"Error checking Spark job status: {e}")
                # handle the exception 
        print("livy status: " + status)
        
        # creating/updating file to trigger lambda
        if(status=='success'):
            s3 = boto3.client('s3')
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            text = f"The Livy job completed at {current_time}"
            s3.put_object(Body=text,Bucket=S3_STAGING_BUCKET,Key=LAMBDA_TRIGGER)


        # terminate cluster once the spark job has been submitted
        emr = boto3.client('emr', region_name=REGION)
        try:
            response = emr.terminate_job_flows(JobFlowIds=[job_flow_id])
        except Exception as e:
            print(f"Error occurred while terminating EMR cluster {job_flow_id}: {str(e)}")
        else:
            print(f"EMR cluster {job_flow_id} has been terminated successfully.")

    
# POST VALIDAITON OF DATA 
def post_validation(ti):
    
    # app_config and necessary location
    try:
        config = ti.xcom_pull(task_ids='read_file_from_s3')
    except Exception as e:
        raise AirflowException(f"Failed to fetch xcom data from 'read_file_from_s3' task: {e}")
        
    source_location = config["transformation-dataset"]['source']['data-location']
    destination_location = config["transformation-dataset"]['destination']['data-location']
    file_format = config["transformation-dataset"]['source']['file-format']
    datasets = config["transformation-dataset"]['datasets']
    
    # source and destination bucket and location key
    source_bucket_name, source_key = source_location.split('s3://')[1].split('/', 1)
    
    # store landing zone data
    dfs_LZ = {}
    s3 = s3fs.S3FileSystem()
    for dataset in datasets:
        file_key = source_key + dataset
        s3_file_path = "s3://{}/{}".format(source_bucket_name, file_key)
        try:
            parquet_file = pq.ParquetDataset(s3_file_path, filesystem=s3).read_pandas().to_pandas()
        except Exception as e:
            print(f"Error reading file from landing zone for validation {s3_file_path}: {e}")
        dfs_LZ[dataset] = parquet_file
    
    # store staging zone data
    for dataset in datasets:
        file_pattern = f"{destination_location}{dataset}/month_part=*/date_part=*/*.parquet"
        dataset_files = s3.glob(file_pattern)

        # read all Parquet files and concatenate them into a single DataFrame
        dfs = []
        all_data=pd.DataFrame()
        for file in dataset_files:
            try:
                table = pq.read_table(f"s3://{file}", filesystem=s3)
            except Exception as e:
                print(f"Error reading file from staging zone for validation {file}: {e}")
            df = table.to_pandas()
            dfs.append(df)
            all_data = pd.concat(dfs)
            
        # verify columns for dataset
        df1 = dfs_LZ[dataset]
        df2 = all_data

        # compare number of rows
        print("Dataset: ", dataset)
        if len(df1.columns) != len(df2.columns):
            df1_cols = set(df1.columns)
            df2_cols = set(df2.columns)
            diff_cols = df1_cols.symmetric_difference(df2_cols)
            print(f"Data inconsistency found. The following columns are not present in both datasets: {diff_cols}")
        else:
            print("All columns are present")
        # compare if all columns in both dataset exist within each other for comparision
        #exclude_cols = ['month', 'date']
        #for column in [col for col in df1.columns if col not in exclude_cols]:
        for column in df1.columns:
            if column not in df2.columns:
                print("Column " + column + " does not exist in the staging zone dataset.")
                continue
            # column-wise validation 
            if len(df1[column]) == len(df2[column]) and df1[column].dtype == df2[column].dtype:
                print("Column: " + column + ", Records: Matched, Datatype: Matched" )
            if len(df1[column]) != len(df2[column]) and df1[column].dtype == df2[column].dtype:
                print("Column: " + column + ", Records: Miss-matched, Datatype: Matched")
                print("Records in landing zone dataset: " + str(len(df1[column])))
                print("Records in staging zone dataset: " + str(len(df2[column])))
            if len(df1[column]) == len(df2[column]) and df1[column].dtype != df2[column].dtype:
                print("Column: " + column + ", Records: Matched, Datatype: Miss-matched")
                print("Datatype in landing zone dataset: " + str(df1[column].dtype))
                print("Datatype in staging zone dataset: " + str(df2[column].dtype))
            if len(df1[column]) != len(df2[column]) and df1[column].dtype != df2[column].dtype:
                print("Column: " + column + ", Records: Miss-matched, Datatype: Miss-matched")
                print("Records in landing zone dataset: " + str(len(df1[column])))
                print("Records in staging zone dataset: " + str(len(df2[column])))
                print("Datatype in landing zone dataset: " + str(df1[column].dtype))
                print("Datatype in staging zone dataset: " + str(df2[column].dtype))
    


# DAG 
default_args = {
    'owner': 'advait',
    'start_date': datetime(2023, 3, 21),
    'email_on_failure': True,
    'email': 'snehasri1227@gmail.com',
    'on_failure_callback': send_email_notification
    }

dag = DAG(
    'cubs',
    default_args=default_args,
    schedule_interval=None
    )

# Tasks
#Reading file from S3
read_file_from_s3 = PythonOperator(
    task_id='read_file_from_s3',
    python_callable=read_file_from_s3,
    provide_context=True,
    dag=dag
    )

# Creating a EMR cluster
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_cluster,
    op_kwargs={
        'S3_BUCKET_NAME': S3_BUCKET_NAME,
        'SPARK_CONFIG': SPARK_CONFIG,
        'REGION': REGION,
        'KEY_NAME': KEY_NAME,
        'INSTANCE_TYPE':INSTANCE_TYPE,
        'MASTER_NODE': MASTER_NODE,
        'WORKER_NODE': WORKER_NODE
    },
    provide_context=True,
    dag=dag
    )

# Submitting spark job using livy
submit_spark_job = PythonOperator(
    task_id='submit_spark_job',
    python_callable=submit_spark_job,
    op_kwargs={
        'REGION': REGION,
        'S3_STAGING_BUCKET': S3_STAGING_BUCKET,
        'LAMBDA_TRIGGER':LAMBDA_TRIGGER
    },
    provide_context=True,
    dag=dag,
    )

# Postvalidating the dataset in staging zone
post_validation = PythonOperator(
    task_id='post_validation',
    python_callable=post_validation,
    provide_context=True,
    dag=dag,
    )



read_file_from_s3>>create_cluster>>submit_spark_job>>post_validation