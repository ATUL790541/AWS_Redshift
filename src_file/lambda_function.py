import json
import psycopg2
import os

def lambda_handler(event, context):
        dbname = os.environ["dbname"]
        host = os.environ["host"]
        user = os.environ["user"]
        password = os.environ["password"]
        tablename = os.environ["tablename"]
        schema=os.environ["schema"]
        iam_role=os.environ["iam_role"]
        source=os.environ["source"]
        
        file=open("tableschema.json")
        create_table_query=json.load(file)
        try:
            connection = psycopg2.connect(dbname = dbname,
                                       host = host,
                                       port = '5439',
                                       user = user,
                                       password = password)
            curs = connection.cursor()
            print("Connection to database is established")
        except:
            raise Exception("Unable to connect to cluster")

        try:
            query=create_table_query["schema"]
            curs.execute(query)
        except:
            raise Exception("Unable to create the table")
        try:    
            query = f"COPY {schema}.{tablename} FROM '{source}' IAM_ROLE '{iam_role}' FORMAT AS PARQUET;"
            curs.execute(query)
            connection.commit()
            curs.close()
            connection.close()
            print('Data has been populated in the table')
        except:
            raise Exception("Error while loading data into the table")
            
        return "200"

