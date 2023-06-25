# Install Dependencies
pip install faker
pip install pyarrow

import pandas as pd
import numpy as np
import faker
from random import randint
from faker import Faker
from faker.providers import DynamicProvider
fake=Faker(['en-PH','en_US','az-AZ'])
import datetime as dt
from datetime import datetime


# Create a new category-LOCATION_CATEGORY
location_category= DynamicProvider(
     provider_name="location_category",
     elements=["continents","islands","countries","regions","state","cities","neighborhoods","villages"],
    )
#create a new category-LOCATION-GRANULARITY
location_granularity=DynamicProvider(provider_name="location_granularity",elements=["urban","rural","suburban"])
#create a new location_source
location_source=DynamicProvider(provider_name="location_source",elements=['A','B','C','D','E','F','G','H'])

fake=Faker()
fake.add_provider(location_category)
fake.add_provider(location_granularity)
fake.add_provider(location_source)

# Adding column as per the schema
advertising_id= [x for x in range(1,10000001)]
user_ids = [x for x in range(5000,5000000)]
iterator1 = iter(advertising_id)
iterator2 = iter(user_ids)

# Function to define size of the database
def number_of_entries(x):
    
    active_data={}
    for i in range(0,x):
        active_data[i]={}
        active_data[i]['advertising_id']=str(next(iterator1))
        active_data[i]['city']=fake.city()
        active_data[i]['location_category']=fake.location_category()
        active_data[i]['location_granularities']=fake.location_granularity()
        active_data[i]['location_source']=list([fake.location_source(),fake.location_source()])
        active_data[i]['state']=fake.state()
        active_data[i]['timestamp']=fake.date_time()
        active_data[i]['user_id']=str(next(iterator2))
        active_data[i]['user_latitude']=float(fake.latitude())
        active_data[i]['user_longitude']=float(fake.longitude())
        active_data[i]['date']=fake.date_between_dates(date_start=datetime(2022,1,1), date_end=datetime(2022,12,31))
        active_data[i]['month']=str(active_data[i]['date'].month)
        
    return(active_data)
    
# User defined entries
entries=1000
active_data=number_of_entries(entries)

# Data
df=pd.DataFrame.from_dict(active_data).transpose()
df['timestamp']=pd.to_datetime(df['timestamp']).values.astype('long')

# to csv
df.to_csv('actives_1l.csv', header=True, index=False)

# to parquet
df.to_parquet('actives_1l.parquet',index=False)

# check the parquet file
parquet_file=pd.read_parquet('actives_1l.parquet')
parquet_file