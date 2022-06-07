# Databricks notebook source
# MAGIC %md-sandbox #Collaborate via Notebooks
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/07/Logos-1.png" width=500 />
# MAGIC 
# MAGIC #####Key Benefits
# MAGIC 
# MAGIC * Multi-language support
# MAGIC * Interactive Visualizations
# MAGIC * Real-time coauthoring
# MAGIC * git integration
# MAGIC * Scheduler
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/06/blog-ds-workspace-3.png" width=400/>
# MAGIC <br>
# MAGIC 
# MAGIC #####Citi Bike Use-Case Review
# MAGIC 
# MAGIC <br>
# MAGIC <img src="https://github.com/vspw/db_notebooks_misc/blob/main/ingestraw.png?raw=true" width=200/>

# COMMAND ----------

import os, time
import requests
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField

# COMMAND ----------

# dbutils.fs.rm('/home/venkata.wunnava@databricks.com/datasets/citi-bike/bronze/stream_station_raw/',True)
# dbutils.fs.rm('/home/venkata.wunnava@databricks.com/datasets/citi-bike/bronze/stream_station_chkpt/',True)
# dbutils.fs.rm('/home/venkata.wunnava@databricks.com/datasets/citi-bike/silver/stream_station_chkpt/',True)

# COMMAND ----------

# Config section:
#----------------
# Streaming Paths:
stream_station_raw = '/home/venkata.wunnava@databricks.com/datasets/citi-bike/bronze/stream_station_raw'
# Create folders if they dont exist:
dbutils.fs.mkdirs(stream_station_raw)
# Define Realtime CitiBike API URLs:
station_status = "https://gbfs.citibikenyc.com/gbfs/en/station_status.json"

# COMMAND ----------

  ## manually triggered task to create New files in raw/landing zone.  
  
  now = datetime.now() # current date and time
  fmt_now = now.strftime("%Y%m%d_%H-%M-%S")
  #print("date and time:",fmt_now)	
  # Create the JSON file:
  try:
    print('---------------------------------------------------')
    print('Creating empty JSON response file at start of Loop.')
    dbutils.fs.put(f"{stream_station_raw}/station_status_{fmt_now}.json", "")
  except:
    print('File already exists')
  
  # Call API & get JSON response:
  #----------------------------------------------------------
  resp = requests.get(station_status)
  if resp.status_code != 200:
      # This means something went wrong
      raise ApiError(f'GET /tasks/ {resp.status_code}')

  print("Response Status Code : ", resp.status_code)
  resp_json_str = resp.content.decode("utf-8")
  print("Byte size of JSON Response: ", len(resp_json_str))
  #----------------------------------------------------------
  
  with open(f"/dbfs/{stream_station_raw}/station_status_{fmt_now}.json","w") as f:
    f.write(resp_json_str)
  
