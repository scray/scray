#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import pandas as pd

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col

def getSparkSession():
    return SparkSession.builder.config('spark.local.dir', '/tmp').config("spark.executor.memory", "8g").config("spark.driver.memory", "8g").config("spark.driver.maxResultSize", "0").appName("jupyter").getOrCreate()
    #return SparkSession.builder.config('spark.local.dir', '/tmp').config("spark.executor.memory", "8g").config("spark.driver.memory", "8g").config("spark.driver.maxResultSize", "0").appName("jupyter").master("spark://clspromon-aio01.txx.seeburger.de:7077").getOrCreate()
    #return SparkSession.builder.config('spark.local.dir', '/tmp').config("spark.executor.memory", "8g").config("spark.driver.memory", "8g").config("spark.driver.maxResultSize", "0").appName("example-pyspark-read-and-write").appName('jupyter').master('spark://clspromon-aio01.txx.seeburger.de:7077').getOrCreate()
    #return SparkSession.builder.config('spark.local.dir', '/tmp').config("spark.executor.memory", "8g").config("spark.driver.memory", "8g").config("spark.driver.maxResultSize", "0").appName("example-pyspark-read-and-write").appName('jupyter').master('spark://172.30.17.145:7077').getOrCreate()

