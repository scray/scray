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
    return SparkSession.builder.config('spark.local.dir', '/tmp').config("spark.executor.memory", "8g").config("spark.driver.memory", "8g").config("spark.driver.maxResultSize", "0").appName("example-pyspark-read-and-write").getOrCreate()

