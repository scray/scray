import numpy as np
import pandas as pd

from pyspark.sql.functions import col, udf, mode, count, avg, when, min, max, when, rand
import time
import datetime

import DataCollector as collector
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import PCA

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from IPython.display import display, Markdown
import shutil
import os

class FeaturePreparer():
    start = None
    range = 144000 #4h

    test_path_1 = 'new_message_tracking/data_1/test.parquet'
    train_path_1 = 'new_message_tracking/data_1/train.parquet'
    df_path_0 = 'new_message_tracking/data/train.parquet'
    pca_feature_columns = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE', 'year', 'month', 'day', 'hour', 'minute']
    pca_columns = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE', 'year', 'month', 'day', 'hour', 'minute', 'error']

#--------------------------------------------------------------------------------------------- PREPARATION FOR TRAINING 1 --------------------------------------------------------------------------------------------------------------

    #---------------------------------------------------------
    def prepare_data_for_training_1(self, raw_data):         #
        data = self.t1_convert_to_features_column(raw_data)  #
        return data                                          #
    #---------------------------------------------------------

    #----------------------------- MAIN FUNCTIONS
    
    def t1_convert_to_features_column(self, data):
        features = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','year', 'month', 'day', 'hour', 'minute']
        assembler = VectorAssembler(inputCols=features, outputCol='features', handleInvalid='keep')
        data = assembler.transform(data)
        data = data.select('features', 'error')
        return data

    #--------------------------------------------------------------------------------
    def prepare_data_for_training_1_presplit(self, raw_data, distribution):         #
        data = self.t1_convert_to_features_column_presplit(raw_data, distribution)  #
        return data                                                                 #
    #--------------------------------------------------------------------------------

    #----------------------------- MAIN FUNCTIONS
    
    def t1_convert_to_features_column_presplit(self, data, distribution):
        features = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','year', 'month', 'day', 'hour', 'minute']
        assembler = VectorAssembler(inputCols=features, outputCol='features', handleInvalid='keep')
        data = assembler.transform(data)
        data = data.select('features', 'error')

        data_0 = data.filter(data.error == 0)
        data_1 = data.filter(data.error == 1)

        train_0, test_0 = data_0.randomSplit([0.8, 0.2], seed=1)
        train_1, test_1 = data_1.randomSplit([0.8, 0.2], seed=1)

        train_data = train_0
        
        len = train_1.count()
        print(len)
        counter = 0
        while counter < distribution:
            train_data = train_data.union(train_1)
            df = train_0.limit(len)
            train_0 = train_0.subtract(df)
            counter = counter +1
            #if train_1.count() <= len:
            #    break

        test_data = test_0.union(test_1)
        train_data = train_0.union(train_data)
        
        train_data = train_data.orderBy(rand())
        test_data = test_data.orderBy(rand())

        train_data.write.mode('overwrite').parquet(self.train_path_1)
        test_data.write.mode('overwrite').parquet(self.test_path_1)
        
        return train_data

    
    def t1_convert_to_features_column_presplit_from_previous(self, spark, distribution):
        train_data = spark.read.parquet(self.train_path_1)

        train_0 = train_data.filter(train_data.error == 0)
        train_1 = train_data.filter(train_data.error == 1)

        train_data = train_0
        df_1 = spark.read.parquet(self.df_path_0)
        features = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','year', 'month', 'day', 'hour', 'minute']
        assembler = VectorAssembler(inputCols=features, outputCol='features', handleInvalid='keep')
        df_1 = assembler.transform(df_0)
        df_1 = df_1.select('features', 'error')
        
        len = 247205
        counter = 0
        while counter < distribution:
            train_data = train_data.union(df_1)
            df = train_0.limit(len)
            train_0 = train_0.subtract(df)
            counter = counter +1

        train_data = train_0.union(train_data)
        
        train_data = train_data.orderBy(rand())

        train_data.write.mode('overwrite').parquet(self.train_path_1)
        
        return train_data

    #--------------------PCA

    #--------------------------------------------------------------------------
                                                                              #
    def prepare_data_for_training_1_pca(self, data):                          #
        self.printmd('# Principal Component Analysis (PCA)')                  #
        return self.performPCA(data)                                          #
        print("")                                                             #
                                                                              #
    #--------------------------------------------------------------------------

    def performPCA(self, data):
        no_str_df = data[self.pca_columns]
        vector_col = "pca_vars"
        assembler = VectorAssembler(inputCols=self.pca_feature_columns, outputCol=vector_col)
        df_vector = assembler.transform(no_str_df).select(vector_col, 'error')
        pca = PCA(k=2, inputCol=vector_col, outputCol="pcaFeatures")
        model = pca.fit(df_vector)
        result = model.transform(df_vector).select("pcaFeatures", "error")

        components = model.pc
        rows = components.toArray().tolist()
        df = self.getSparkSession().createDataFrame(rows,['PC1','PC2','PC3'])
        df = df.toPandas().transpose()
        df.columns = self.pca_columns
        display(df)
        df.to_pickle('work/new_message_tracking/models/pca_original_components.pkl')

        '''
        ev = model.explainedVariance
        df = pd.DataFrame(data = ev,
                         columns = ['Principle Components'])
        fig = px.bar(df, y=df['Principle Components'])
        fig.show()
        '''
        
        cumValues = model.explainedVariance.cumsum()
        plt.figure(figsize=(10,8))
        plt.plot(range(1,3), cumValues, marker = 'o', linestyle='--')
        plt.title('variance by components')
        plt.xlabel('num of components')
        plt.ylabel('cumulative explained variance')

        return result

#--------------------------------------------------------------------------------------------- PREPARATION FOR TRAINING 2 --------------------------------------------------------------------------------------------------------------

    #----------------------------------------------------------
    def prepare_data_for_training_2(self, raw_data):          #
        data = self.t2_convert_to_features_column(raw_data)   #
        return data                                           #
    #----------------------------------------------------------

    #----------------------------- MAIN FUNCTIONS

    def t2_convert_to_features_column(self, data):
        features = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','year', 'month', 'day', 'hour', 'minute']
        assembler = VectorAssembler(inputCols=features, outputCol='features', handleInvalid='keep')
        data = assembler.transform(data)
        data = data.select('features', 'CMESSAGETAT2')
        return data
        
#--------------------------------------------------------------------------------------------- PREPARATION FOR TRAINING 3 --------------------------------------------------------------------------------------------------------------

    #--------------------------------------------------------
    def prepare_data_for_training_3(self, raw_data):        #
        data = self.t3_first_steps(raw_data)                #
        data = self.t3_add_range_column(data)               #
        data = self.t3_add_aggregated_columns(data)         #
        data = self.t3_filter_count(data)                   #
        data = self.t3_add_traffic_column(data)             #
        #self.t3_print_traffic_statistics(data)              #
        data = self.t3_convert_to_features_column(data)     #
        return data                                         #
    #--------------------------------------------------------

    #----------------------------- MAIN FUNCTIONS

    def t3_first_steps(self, data):
        data = data['CGLOBALMESSAGEID','CSTARTTIME','CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','year', 'month', 'day', 'hour', 'minute', 'CSLATAT','CSTATUS']
        self.start = data.select(min('CSTARTTIME')).first()['min(CSTARTTIME)']
        return data

    def t3_add_range_column(self, data):
        get_range_UDF = udf(lambda z: self.get_range(z))
        data = data.withColumn('range', get_range_UDF(col("CSTARTTIME")))
        return data
        
    def t3_add_aggregated_columns(self, data):
        get_percentage_UDF = udf(lambda x, y: self.get_percentage(x,y))
        data = data.groupBy('range','CSERVICE').agg(
           count('CGLOBALMESSAGEID').alias('count'),
           mode('CSENDERENDPOINTID').alias('CSENDERENDPOINTID_mode'),
           mode('CSENDERPROTOCOL').alias('CSENDERPROTOCOL_mode'),
           avg('CINBOUNDSIZE').alias('CINBOUNDSIZE_avg'),
           mode('year').alias('year_mode'),
           mode('month').alias('month_mode'),
           mode('day').alias('day_mode'),
           get_percentage_UDF(count(when(col('CSTATUS') == 14, True)), count('CSTATUS')).alias('STATUS_percentage'),
           avg('CSLATAT').alias('CSLATAT_avg'),
           min('CSLATAT').alias('CSLATAT_min'))
        data = data.withColumn('CSLATAT_avg', data.CSLATAT_avg.cast('int'))
        return data

    def t3_filter_count(self, data):
        data = data.filter(data['count'] > 100)
        return data
        
    def t3_add_traffic_column(self,data):
        traffic_light_condition_UDF = udf(lambda x,y,z: self.traffic_light_condition(x,y,z))
        data = data.withColumn('traffic_light',traffic_light_condition_UDF(data.STATUS_percentage, data.CSLATAT_avg, data.CSLATAT_min))
        data = data.withColumn('traffic_light', data.traffic_light.cast('int'))
        return data

    def t3_print_traffic_statistics(self, data):
        count = data.groupBy('traffic_light').count()
        count = count.withColumn('traffic_light_str', when(col('traffic_light') == 0, 'Grün').otherwise('Rot'))
        count_pd = count.toPandas()
        fig = px.bar(count_pd, y='count', x='traffic_light_str', title="Verteilung von Ampelfarben" , height=450)
        print(fig.show())

    def t3_convert_to_features_column(self, data):
        features = ['CSERVICE','CSENDERENDPOINTID_mode','CSENDERPROTOCOL_mode','CINBOUNDSIZE_avg','year_mode','month_mode','day_mode', 'count']
        assembler = VectorAssembler(inputCols=features, outputCol='features', handleInvalid='keep')
        data = assembler.transform(data)
        data = data.select('features', 'traffic_light')
        return data

    #----------------------------- HELPER FUNCTIONS
    
    def get_range(self, time):
        return int((time - self.start)/self.range)

    def get_percentage(self, item_count, all_count):
        return int((item_count/all_count)*100)

    def traffic_light_condition(self, STATUS_percentage, CSLATAT_avg, CSLATAT_min):
        if((int(STATUS_percentage) < 95) | (int(CSLATAT_avg) > 30000) | (int(CSLATAT_min) < 0)):
            return 1
        return 0

    def printmd(self, string):
        display(Markdown(string))

