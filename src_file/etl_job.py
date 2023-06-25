# import
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sha2
from pyspark.sql.functions import *
import json
from pyspark.sql.functions import concat_ws
import boto3


class RisingCubsETLJob:
    def __init__(self, spark):
        self.spark = spark

    def read_data(self, source_path, val, file_format):
        """
        Check if landing bucket exists or not then read the data if exists

        :param source_path: Path of source bucket
        :param val: Name of Dataset in S3
        :param file_format: Format of dataset to be written
        :return: DataFrame from S3
        :rtype: Spark DataFrame
        """
        s3 = boto3.resource("s3")
        bucket = s3.Bucket("rising-cubs-landing-bucket")
        if bucket.creation_date:
            print("The Landing bucket exists")
        else:
            raise ValueError("Bucket not found")
        print("started reading")
        dataframe = spark.read.parquet(source_path + val)
        print("finished reading")
        dataframe.printSchema()
        dataframe.show(5, False)

        print("Printing--------------------")
        print(dataframe.count())
        print(dataframe.rdd.getNumPartitions())
        return dataframe

    def write_data(self, dataframe, path, val, file_format, partition):
        """
        Check if staging bucket exists or not then write the data if exists

        :param dataframe: DataFrame used for transformation
        :param path: Path of staging Bucket
        :param val: Dataset Name
        :param file_format: Format of dataset to be written
        :param partition: Partition columns
        """
        s3 = boto3.resource("s3")
        bucket = s3.Bucket("rising-cubs-staging-bucket")
        if bucket.creation_date:
            print("The Staging bucket exists")
        else:
            raise ValueError("Bucket not found")
        print("start writing")
        dataframe.show(5, False)
        dataframe.write.mode("overwrite").partitionBy(partition).format(
            file_format
        ).save(path + val)
        print("finished writing")

    def test_col(self, app_config_value, dataframe):
        """
        Check if the columns in app_config present in dataset or not

        :param app_config_value: column name from app_config
        :param dataframe: DataFrame used for transformation
        :return: If the column present or not
        :rtype: bool
        """
        check_column_status = True
        for cols in app_config_value:
            if app_config_value[cols]["name"] in dataframe.columns:
                print(f"{app_config_value[cols]['name']} present")
            else:
                check_column_status = False
                raise ValueError(f"{app_config_value[cols]} column not present")
        return check_column_status

    def check_col(self, dataframe):
        """
        Check for each column group if the column present or not

        :param dataframe: DataFrame used for transformation
        :return: If all column present or not
        :rtype: bool
        """
        print("Started checking columns")
        check_column_status = True
        check_column_status = self.test_col(mask, dataframe)
        check_column_status = self.test_col(dec, dataframe)
        check_column_status = self.test_col(comma, dataframe)
        print("Column Checking Finished")
        return check_column_status

    def masking_columns(self, dataframe):
        """
        Masking the required column

        :param dataframe: DataFrame used for transformation
        :return: DataFrame from S3
        :rtype: Spark DataFrame
        """
        print("Started Masking")
        for cols in mask:
            dataframe = dataframe.withColumn(
                "masked_" + mask[cols]["name"],
                sha2(col(mask[cols]["name"]).cast("binary"), 256),
            )
            print(f"{mask[cols]['name']} masked")
        print("Masking Finished")
        return dataframe

    def change_type(self, dataframe):
        """
        Decimal transformation then transforming column in comma seperated string

        :param dataframe: DataFrame used for transformation
        :return: DataFrame from S3
        :rtype: Spark DataFrame
        """
        print("Started decimal transformation")
        for cols in dec:
            dataframe = dataframe.withColumn(
                dec[cols]["name"], col(dec[cols]["name"]).cast(dec[cols]["to"])
            )
            print(f"{dec[cols]['name']} transformed to decimal")
        print("Decimal transformation finished")

        print("Started comma transformation")
        for cols in comma:
            dataframe = dataframe.withColumn(
                comma[cols]["name"],
                concat_ws(comma[cols]["to"], col(comma[cols]["name"])),
            )
            print(f"{comma[cols]['name']} transformed to csv")
        print("Comma transformation finished")
        return dataframe


def main(spark):
    rising_cubs_etl_obj = RisingCubsETLJob(spark)

    """
    Reading the data from s3
    """
    dataframe = rising_cubs_etl_obj.read_data(landing_path, datasets, file_format)

    """
    Checking if all required columns present or not
    """
    bool_check = rising_cubs_etl_obj.check_col(dataframe)
    if bool_check:
        print("All required columns present")
    else:
        raise ValueError(f"All required columns not present")

    """
    Required Transformation of all columns
    """
    dataframe = rising_cubs_etl_obj.masking_columns(dataframe)
    dataframe = rising_cubs_etl_obj.change_type(dataframe)
    dataframe.printSchema()
    dataframe.show(5, False)

    """
    Writing data using partition columns
    """
    dataframe = dataframe.withColumn("month_part", col("month"))
    dataframe = dataframe.withColumn("date_part", col("date"))
    rising_cubs_etl_obj.write_data(
        dataframe, staging_path, datasets, file_format, partition
    )


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Hackathon").getOrCreate()

    """
    Reading the app_config from s3 that contains all paths and columns name used in Code
    """
    app_config = (
        spark.read.option("multiline", "true")
        .json("s3://rising-cubs-landing-bucket/config_file/app_config.json")
        .toJSON()
        .collect()
    )

    """
    Declaring all the variables required in code

    :param app_config: Appconfig loaded converted to dictionary
    :param landing_path: Path of landing bucket in s3
    :param staging_path: Path of staging bucket in s3
    :param partition: Partition column from app_config
    :param datasets: Name of dataset in s3 from app_config
    :param file_format: Format of dataset file in s3 from app_config
    :param mask: Columns to be masked from app_config
    :param dec: Columns to be decimal transformed from app_config
    :param comma: Columns to be made comma seperated string from app_config
    """
    app_config = json.loads(app_config[0])
    landing_path = app_config["transformation-dataset"]["source"]["data-location"]
    staging_path = app_config["transformation-dataset"]["destination"]["data-location"]
    partition = app_config["transformation-dataset"]["partition-cols"]
    datasets = app_config["transformation-dataset"]["datasets"][0]
    file_format = app_config["transformation-dataset"]["source"]["file-format"]
    mask = app_config["transformation-dataset"]["masking-cols"]["mask"]
    dec = app_config["transformation-dataset"]["transformation-cols"]["to_decimal"]
    comma = app_config["transformation-dataset"]["transformation-cols"]["to_comma"]
    main(spark)
