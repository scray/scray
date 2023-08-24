import numpy as np
import pandas as pd
from collections import OrderedDict

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col
from pyspark.ml.feature import Imputer
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
import sys

import pyspark
from pyspark.rdd import RDD
from pyspark.sql import Row
from pyspark.sql import DataFrame
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark.pandas as ps
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from IPython.display import display, Markdown, Latex, display_markdown

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import matplotlib.pyplot as plt

import Encoder as enc

class DataCollector():
#--------------------------------------------------------------CLASS VARIABLES-------------------------------------------------------------------------

    version_sla = 'v00001'
    version     = version_sla + '/v00000'
    home_directory  =  '/home/jovyan/work/'
    share_directory =  '/home/jovyan/share/'
    directory = 'hdfs://172.30.17.145:8020/'
    npy = None
    sample_path = 'new_message_tracking/data/sample.parquet'
    sample_path_cleaned = 'new_message_tracking/data/sample_cleaned.parquet'
    data_shuffled_path = 'new_message_tracking/data/shuffled_data.parquet'
    all_path_cleaned = 'new_message_tracking/data/all_data.parquet'

    string_columns = ['CGLOBALMESSAGEID']
    numeric_columns = ['CSTARTTIME','CENDTIME','CSTATUS','CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','CRECEIVERPROTOCOL','CRECEIVERENDPOINTID','CSLATAT', 'CMESSAGETAT2', 'CSLADELIVERYTIME', 'year', 'month', 'day', 'hour', 'minute', 'outcome']
    inherent_numeric_columns = ['CINBOUNDSIZE','CSLATAT', 'CMESSAGETAT2']
    negative_df = None

    def _init_(self):
        self.npy = self.share_directory + 'sla/' + self.version_sla + '/npy'
        np_load_old = np.load
        np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)
    
#-------------------------------------------------------------PIPELINE FUNCTIONS-------------------------------------------------------------------------

    #--------------------LOAD DATA

    #-----------------------------------------
                                             #
    def loadData(self):                      #
        data = self.getData()                #
        #data = self.addOutcomeColumn(data)   #
        return data                          #
                                             #
    #-----------------------------------------

    # returns data
    def getData(self):
        #return self.getSparkSession().read.parquet(self.data_shuffled_path)
        return self.getSparkSession().read.parquet(self.directory + '/user/admin/sla/v00002/v00000/encoded/all/sla_enc_keep_v00002_v00000_all.parquet')

    # returns data
    def addOutcomeColumn(self, data):
        '''
        data = enc.assign_outcome_error(self.npy, data)
        data = data.withColumn("outcome",data.outcome.cast('integer'))
        return data
        '''
        data = data.withColumn("outcome", F.when(F.col('error') == 0, 1).otherwise(0))
        data = data.drop('error')
        return data

    def loadSample(self):
        sample = self.getSparkSession().read.parquet(self.sample_path)
        return sample
    
    def createSample(self):
        data = self.getSparkSession().read.parquet(self.data_shuffled_path)
        sample_data = data.sample(0.1)
        sample_data.write.mode('overwrite').parquet(self.sample_path)

    def get_Data_half_distribution(self):
        data = self.getSparkSession().read.parquet(self.data_shuffled_path)
        data_0 = data.filter(data.error == 1)
        count = data_0.count()
        sample = data.sample(0.1)
        sample_1 = sample.filter(sample.error == 0)
        data_1 = sample_1.limit(count)
        data = data_1.union(data_0)
        return data.orderBy(F.rand())

    def get_Data_third_distribution(self):
        data = self.getSparkSession().read.parquet(self.data_shuffled_path)
        data_0 = data.filter(data.error == 1)
        count = data_0.count()
        sample = self.loadSample()
        sample_1 = sample.filter(sample.error == 0)
        data_1 = sample_1.limit((2*count))
        data = data_1.union(data_0)
        return data.orderBy(F.rand())

    def get_Data_quarter_distribution(self):
        data = self.getSparkSession().read.parquet(self.data_shuffled_path)
        data_0 = data.filter(data.error == 1)
        count = data_0.count()
        sample = self.loadSample()
        sample_1 = sample.filter(sample.error == 0)
        data_1 = sample_1.limit((3*count))
        data = data_1.union(data_0)
        return data.orderBy(F.rand())

    def get_Data_two_third_distribution(self):
        data = self.getSparkSession().read.parquet(self.data_shuffled_path)
        data_0 = data.filter(data.error == 1)
        count = data_0.count()
        sample = self.loadSample()
        sample_1 = sample.filter(sample.error == 0)
        len = int(count/2)
        data_1 = sample_1.limit(len)
        data = data_1.union(data_0)
        return data.orderBy(F.rand())

    def get_Data_three_quarter_distribution(self):
        data = self.getSparkSession().read.parquet(self.data_shuffled_path)
        data_0 = data.filter(data.error == 1)
        count = data_0.count()
        sample = self.loadSample()
        sample_1 = sample.filter(sample.error == 0)
        len = int(count/3)
        data_1 = sample_1.limit(len)
        data = data_1.union(data_0)
        return data.orderBy(F.rand())
    
    #--------------------STATISTICS
    
    #---------------------------------------------------------------------------
                                                                               #
    def getStatistics(self, data):                                             #
        self.printmd('# Statistics')                                           #
        print(self.get_all_false_data(data))                                   #
        self.printmd('**Length of Data**: ' + str(self.getDataLength(data)))   #
        print(self.get_data_cleaning_statistics(data).show())                  #
                                                                               #
    #---------------------------------------------------------------------------
    
    def get_all_false_data(self, data):
        missingData = self.getMissingDataDataframe(data)
        zeroData = data.filter((data['CSLADELIVERYTIME'] == 0))
        negativeData = data.filter((data['CSERVICE']<0) | (data['CSENDERENDPOINTID']<0) | (data['CSENDERPROTOCOL']<0) | (data['CRECEIVERPROTOCOL']<0) | (data['CRECEIVERENDPOINTID']<0) | (data['CSLATAT']<0) | (data['CMESSAGETAT2']<0) | (data['CSLADELIVERYTIME']<0))
        negativeData = data.filter((data['CSERVICE']<0) | (data['CSENDERENDPOINTID']<0))
        duplicatedIds = data.groupBy('CGLOBALMESSAGEID')\
                        .count()\
                        .where(F.col('count') > 1)
        duplicatedData = data.join(duplicatedIds, (data.CGLOBALMESSAGEID == duplicatedIds.CGLOBALMESSAGEID), 'leftsemi')

        counter = 0

        for index, column in enumerate(data.columns):
            if column in self.inherent_numeric_columns:
                counter = counter +1
                mean = data.select(F.mean(column)).collect()[0][0]
                stddev = data.select(F.stddev(column)).collect()[0][0]
                upper_limit = mean + (3*stddev)
                lower_limit = mean - (3*stddev)
                outlierInColumn = data.filter((data[column]<lower_limit) | (data[column]>upper_limit))
                if counter == 1:
                    outlierData = outlierInColumn
                else:
                    outlierData = outlierData.union(outlierInColumn)

        allData = missingData.union(zeroData)
        allData = allData.union(outlierData)
        allData = allData.union(negativeData)
        allData = allData.union(duplicatedData)
        
        duplicatedData.dropDuplicates()
        missingData.dropDuplicates()
        outlierData.dropDuplicates()
        zeroData.dropDuplicates()
        negativeData.dropDuplicates()
        allData.dropDuplicates()

        print("Nullen: " + str(self.zeroCount(allData)))
        
        negativeWithoutDuplicatesData = negativeData.join(duplicatedData, (negativeData.CGLOBALMESSAGEID == duplicatedData.CGLOBALMESSAGEID), 'left_anti')
        nullWithoutDuplicatesData = zeroData.join(duplicatedData, (zeroData.CGLOBALMESSAGEID == duplicatedData.CGLOBALMESSAGEID), 'left_anti')
        NotDuplicated = negativeWithoutDuplicatesData.union(nullWithoutDuplicatesData)
        
        df = [['Duplikate', self.zeroCount(duplicatedData)], ['fehlende Werte', self.zeroCount(missingData)], ['Ausreißer',self.zeroCount(outlierData)], ['0-Werte', self.zeroCount(zeroData)], ['negative Werte', self.zeroCount(negativeData)], ['Gegenprobe', self.zeroCount(NotDuplicated)]]
        df = pd.DataFrame(df, columns=['Kriterium', 'Anteil von Ausfällen in %'])
        fig = px.bar(df, y='Anteil von Ausfällen in %', x='Kriterium', title='Anteile von Ausfällen für fehlerhafte Daten', height=450, text_auto=True)
        fig.add_hline(y=self.zeroCount(allData), line_color="red")
        print(fig.show())
                    
                    
    def get_data_cleaning_statistics(self, data):
        negativeData = data.filter((data['CSERVICE']<0) | (data['CSENDERENDPOINTID']<0) | (data['CSENDERPROTOCOL']<0) | (data['CRECEIVERPROTOCOL']<0) | (data['CRECEIVERENDPOINTID']<0) | (data['CSLATAT']<0) | (data['CMESSAGETAT2']<0) | (data['CSLADELIVERYTIME']<0))
        duplicatedData = data.groupBy('CGLOBALMESSAGEID')\
                .count()\
                .where(F.col('count') > 1)
        zeroData = data.filter((data['CSLADELIVERYTIME'] == 0))
        missingData = self.getMissingDataDataframe(data)
        
        duplicatedIds = duplicatedData.rdd.map(lambda x: x['CGLOBALMESSAGEID']).collect()
        duplicatedIds = list(OrderedDict.fromkeys(duplicatedIds))
        zeroIds = zeroData.rdd.map(lambda x: x['CGLOBALMESSAGEID']).collect()
        zeroIds = list(OrderedDict.fromkeys(zeroIds))
        missingIds = missingData.rdd.map(lambda x: x['CGLOBALMESSAGEID']).collect()
        missingIds = list(OrderedDict.fromkeys(missingIds))
        negativeIds = negativeData.rdd.map(lambda x: x['CGLOBALMESSAGEID']).collect()
        negativeIds = list(OrderedDict.fromkeys(negativeIds))
        
        negativeMatches = list(set(negativeIds).intersection(duplicatedIds))
        zeroMatches = list(set(zeroIds).intersection(duplicatedIds))
        missingMatches = list(set(missingIds).intersection(duplicatedIds))
        
        negativePortion = len(negativeMatches)/len(duplicatedIds)
        onlyDuplicatedPortion = list(set(duplicatedIds) - set(negativeIds))
        zeroPortion = len(zeroMatches)/len(duplicatedIds)
        onlyDuplicatedPortion = list(set(onlyDuplicatedPortion) - set(zeroIds))
        missingPortion = len(missingMatches)/len(duplicatedIds)
        onlyDuplicatedPortion = list(set(onlyDuplicatedPortion) - set(missingIds))
        #onlyDuplicatedPortion = len(onlyDuplicatedPortion)/len(duplicatedIds)
        
        df = [['Anteil negative Werte in Duplikaten', len(negativeMatches)], ['Anteil Null-Werte in Duplikaten', len(zeroMatches)], ['Anteil fehlende Werte in Duplikaten', len(missingMatches)], ['Anteil ausschließlich Duplikate',len(onlyDuplicatedPortion)]]                                                              
        df = pd.DataFrame(df, columns=['Bezeichnung', 'Anzahl'])
        fig = px.pie(df, values='Anzahl', names='Bezeichnung', title='Anteile von Datenfehlern in Duplikaten', height=450)
        return fig
        
        
    #--------------------NEGATIVE VALUES

    #----------------------------------------------------------------------------------------
                                                                                            #
    def negativeValues(self, data):                                                         #
        self.printmd('# Negative Werte')                                                    #
        self.negative_df = self.detectNegativeValues(data)                                  #
        self.printmd('### Negative Werte vor der Datenbereinigung')                         #
        display(self.negative_df)                                                           #
        print(self.visualizeStatisticDataframe(self.negative_df).show())                    #
        data = self.removeNegativeValues(data)                                              #
        df = self.detectNegativeValues(data)                                                #
        self.printmd('### Negative Werte nach der Datenbereinigung')                        #
        display(df)                                                                         #
        print(self.visualizeStatisticDataframe(df).show())                                  #
        print("")                                                                           #
        return data                                                                         #
                                                                                            #
    #----------------------------------------------------------------------------------------

    # returns statistic
    def detectNegativeValues(self, data):
        negative_values = {}
        for index, column in enumerate(data.columns):
            if column in self.numeric_columns:
                negative_count = data.filter(F.col(column) < 0).count()
                negative_values.update({column:negative_count})
        negative_df = pd.DataFrame.from_dict([negative_values])
        return negative_df

    # returns data
    def removeNegativeValues(self, data):
        for column in self.numeric_columns:
            data = data[data[column] >= 0]
        return data
    
    #--------------------ZERO VALUES
    
    #----------------------------------------------------------------------------------------------------
                                                                                                        #
    def zeroValues(self, data):                                                                         #
        self.printmd('# Null Werte')                                                                    #
        count_before = self.countZeroValuesInCSLADELIVERYTIME(data)                                     #
        data = self.removeZeroValuesInCSLADELIVERYTIME(data)                                            #
        count_after = self.countZeroValuesInCSLADELIVERYTIME(data)                                      #
        df = [[count_before, count_after]]                                                              #
        df = pd.DataFrame(df, columns=['Null-Werte vor dem Löschen', 'Null-Werte nach dem Löschen'])    #
        display(df)                                                                                     #
        print(self.visualizeStatisticDataframe(df).show())                                              #
        return data                                                                                     #
                                                                                                        #
    #----------------------------------------------------------------------------------------------------
    
    def countZeroValuesInCSLADELIVERYTIME(self, data):
        return data.filter(data['CSLADELIVERYTIME'] == 0).count()
    
    def removeZeroValuesInCSLADELIVERYTIME(self, data):
        return data.filter(~(data['CSLADELIVERYTIME'] == 0))

    
    #--------------------DUPLICATED ROWS
    
    #--------------------------------------------------------------------------------------------------
                                                                                                      #
    def duplicatedValues(self, data):                                                                 #
        self.printmd('# Duplikate')                                                                   #
        count_before = self.detectDuplicatedRows(data)                                                #
        data = self.removeDuplicatedRows(data)                                                        #
        count_after = self.detectDuplicatedRows(data)                                                 #
        if count_after == None:                                                                       #
            count_after = 0                                                                           #
        df = [[count_before, count_after]]                                                            #
        df = pd.DataFrame(df, columns=['Duplikate vor dem Löschen', 'Duplikate nach dem Löschen'])    #
        display(df)                                                                                   #
        print(self.visualizeStatisticDataframe(df).show())                                            #
        print("")                                                                                     #
        return data                                                                                   #
                                                                                                      #
    #--------------------------------------------------------------------------------------------------
    
    def detectDuplicatedRows(self, data):
        duplicatedRows = data.groupBy('CGLOBALMESSAGEID')\
        .count()\
        .where(F.col('count') > 1)\
        .select(F.sum('count'))
        return duplicatedRows.first()['sum(count)']
    
    def showDuplicatedRows(self, data):
        duplicatedIds = data.groupBy('CGLOBALMESSAGEID')\
        .count()\
        .where(F.col('count') > 1)
        duplicatedIds = duplicatedIds.rdd.map(lambda x: x['CGLOBALMESSAGEID']).collect()
        duplicatedIds = list(OrderedDict.fromkeys(duplicatedIds))
        duplicates = data.filter(data.CGLOBALMESSAGEID.isin(duplicatedIds))
        duplicates_pd = duplicates.toPandas()
        return duplicates
    
    def removeDuplicatedRows(self, data):
        return data.dropDuplicates(['CGLOBALMESSAGEID'])
    
    #--------------------MISSING VALUES

    #---------------------------------------------------------------------------------------
                                                                                           #
    def missingValues(self, data):                                                         #
        self.printmd('# Fehlende Werte')                                                   #
        df = self.detectMissingValues(data)                                                #
        self.printmd('### Fehlende Werte vor der Datenbereinigung')                        #
        display(df)                                                                        #
        print(self.visualizeStatisticDataframe(df).show())                                 #
        data = self.removeMissingValues(data)                                              #
        df = self.detectMissingValues(data)                                                #
        self.printmd('### Fehlende Werte nach der Datenbereinigung')                       #
        display(df)                                                                        #
        print(self.visualizeStatisticDataframe(df).show())                                 #
        print("")                                                                          #
        return data                                                                        #
                                                                                           #
    #---------------------------------------------------------------------------------------

    # returns statistic
    def detectMissingValues(self, data):
        missing_values = {}
        for index, column in enumerate(data.columns):
            if column in self.string_columns:
                missing_count = data.filter(col(column).eqNullSafe(None) | col(column).isNull()).count()
                missing_values.update({column:missing_count})
            if column in self.numeric_columns:
                missing_count += data.where(col(column).isin([None,np.nan])).count()
                missing_values.update({column:missing_count})
        missing_df = pd.DataFrame.from_dict([missing_values])
        return missing_df

    
    def getMissingDataDataframe(self, data):
        for index, column in enumerate(data.columns):
            if column in self.string_columns:
                missing_data = data.filter(F.col(column).eqNullSafe(None) | F.col(column).isNull())
            if column in self.numeric_columns:
                missing_numeric_data = data.where(F.col(column).isin([None,np.nan]))
                missing_data = missing_data.union(missing_numeric_data)
        return missing_data
        
        
    # returns data
    def removeMissingValues(self, data):    
        imputer = Imputer(inputCols= self.numeric_columns,
            outputCols=self.numeric_columns,
            strategy="mode")

        model = imputer.fit(data)
        data = model.transform(data)

        for column in self.numeric_columns:
            data = data.withColumn(column, data[column].cast('integer'))
        return data

    #--------------------OUTLIER VALUES

    #---------------------------------------------------------------------------------------
                                                                                           #
    def outlierValues(self, data):                                                         #
        self.printmd('# Ausreißer-Werte')                                                  #
        df = self.detectOutliers(data)                                                     #
        self.printmd('### Ausreißer-Werte vor der Datenbereinigung')                       #
        display(df)                                                                        #
        print(self.visualizeStatisticDataframe(df).show())                                 #
        print(self.visualizeOutliers(data).show())                                         #
        data = self.removeOutliers(data)                                                   #
        self.printmd('### Ausreißer-Werte nach der Datenbereinigung')                      #
        df = self.detectOutliers(data)                                                     #
        display(df)                                                                        #
        print(self.visualizeStatisticDataframe(df).show())                                 #
        print(self.visualizeOutliers(data).show())                                         #
        print("")                                                                          #
        return data                                                                        #
                                                                                           #
    #---------------------------------------------------------------------------------------

    # returns fig
    def visualizeOutliers(self, data):
        quantileProbabilities = [0.25, 0.5, 0.75]
        quantiles_CINBOUNDSIZE = data.approxQuantile("CINBOUNDSIZE", quantileProbabilities, 0.01)

        quantile25th_CINBOUNDSIZE = quantiles_CINBOUNDSIZE[0]
        median_CINBOUNDSIZE = quantiles_CINBOUNDSIZE[1]
        quantile75th_CINBOUNDSIZE = quantiles_CINBOUNDSIZE[2]
        min_CINBOUNDSIZE = data.agg({'CINBOUNDSIZE': 'min'}).first()['min(CINBOUNDSIZE)']
        max_CINBOUNDSIZE = data.agg({'CINBOUNDSIZE': 'max'}).first()['max(CINBOUNDSIZE)']

        quantiles_CSLATAT = data.approxQuantile("CSLATAT", quantileProbabilities, 0.01)

        quantile25th_CSLATAT = quantiles_CSLATAT[0]
        median_CSLATAT = quantiles_CSLATAT[1]
        quantile75th_CSLATAT = quantiles_CSLATAT[2]
        min_CSLATAT = data.agg({'CSLATAT': 'min'}).first()['min(CSLATAT)']
        max_CSLATAT = data.agg({'CSLATAT': 'max'}).first()['max(CSLATAT)']

        quantiles_CMESSAGETAT2 = data.approxQuantile("CMESSAGETAT2", quantileProbabilities, 0.01)

        quantile25th_CMESSAGETAT2 = quantiles_CMESSAGETAT2[0]
        median_CMESSAGETAT2 = quantiles_CMESSAGETAT2[1]
        quantile75th_CMESSAGETAT2 = quantiles_CMESSAGETAT2[2]
        min_CMESSAGETAT2 = data.agg({'CMESSAGETAT2': 'min'}).first()['min(CMESSAGETAT2)']
        max_CMESSAGETAT2 = data.agg({'CMESSAGETAT2': 'max'}).first()['max(CMESSAGETAT2)']

        fig = make_subplots(rows=1, cols=3)
        fig.add_trace(go.Box(q1=[quantile25th_CINBOUNDSIZE], median=[median_CINBOUNDSIZE], q3=[quantile75th_CINBOUNDSIZE], lowerfence=[min_CINBOUNDSIZE],
                          upperfence=[max_CINBOUNDSIZE], name="CINBOUNDSIZE"), row=1, col=1)
        fig.add_trace(go.Box(q1=[quantile25th_CSLATAT], median=[median_CSLATAT], q3=[quantile75th_CSLATAT], lowerfence=[min_CSLATAT],
                          upperfence=[max_CSLATAT], name="CSLATAT"), row=1, col=2)
        fig.add_trace(go.Box(q1=[quantile25th_CMESSAGETAT2], median=[median_CMESSAGETAT2], q3=[quantile75th_CMESSAGETAT2], lowerfence=[min_CMESSAGETAT2],
                          upperfence=[max_CMESSAGETAT2], name="CMESSAGETAT2"), row=1, col=3)
        fig.update_yaxes(range=[-1000,25000], row=1, col=1)
        fig.update_yaxes(range=[-1000,25000], row=1, col=2)
        fig.update_yaxes(range=[-1000,25000], row=1, col=3)

        fig.update_xaxes(title_text="CINBOUNDSIZE", showticklabels=False, row=1, col=1)
        fig.update_xaxes(title_text="CSLATAT", showticklabels=False, row=1, col=2)
        fig.update_xaxes(title_text="CMESSAGETAT2", showticklabels=False, row=1, col=3)
        return fig

    # returns statistic
    def detectOutliers(self, data):
        outlier_values = {} 
        for index, column in enumerate(data.columns):
            if column in self.inherent_numeric_columns:
                mean = data.select(F.mean(column)).collect()[0][0]
                stddev = data.select(F.stddev(column)).collect()[0][0]
                upper_limit = mean + (3*stddev)
                lower_limit = mean - (3*stddev)
                outlier_count = data.filter((data[column]<lower_limit) | (data[column]>upper_limit)).count()
                outlier_values.update({column:outlier_count})
        outlier_df = pd.DataFrame.from_dict([outlier_values])
        return outlier_df

    # returns data
    def removeOutliers(self, data):
        for col in self.inherent_numeric_columns:
            mean = data.select(F.mean(col)).collect()[0][0]
            stddev = data.select(F.stddev(col)).collect()[0][0]
            upper_limit = mean + (3*stddev)
            lower_limit = mean - (3*stddev)
            data = data.filter((data[col]>lower_limit) | (data[col]<upper_limit))
        return data

    #--------------------EXPLORATORY DATA ANALYSIS (EDA)

    #--------------------------------------------------------------------------------------
                                                                                          #
    def eda(self, data):                                                                  # 
        self.printmd('# Exploratory Data Analysis (EDA)')                                 #
        self.printmd('### Anzahlen')                                                      #
        self.printmd('**Length of Data**: ' + str(self.getDataLength(data)))              #
        self.printmd('**Count of Senders**: ' + str(self.getSendersCount(data)))          #
        self.printmd('### Spalten-Datentypen')                                            #
        display(self.getColumnDatatypes(data))                                            #
        self.printmd('### Korrelation von Merkmalen')                                     #
        display(self.visualizeCorrelation(data).style.background_gradient(cmap='Blues'))  #
        self.printmd('### Statistiken')                                                   #
        print(self.getStatistic_1(data).show())                                           #
        print(self.getStatistic_2(data).show())                                           #
        print(self.getStatistic_3(data).show())                                           #
        print(self.getStatistic_4(data).show())                                           #
        print(self.getStatistic_5(data).show())                                           #
        print(self.getStatistic_6(data).show())                                           #
        print(self.getStatistic_7(data).show())                                           #
        print(self.getStatistic_8(data).show())                                           #
        print(self.getStatistic_9(data).show())                                           #
        print(self.getStatistic_10(data).show())                                          #
        print(self.getStatistic_11(data).show())                                          #
        print(self.getStatistic_12(data).show())                                          #
        print("")                                                                         #
                                                                                          #
    #--------------------------------------------------------------------------------------

    def getDataLength(self, data):
        return data.count()
    
    # returns statistic
    def getSendersCount(self, data):
        senders = data.select('CSENDERENDPOINTID').dropDuplicates().toPandas()['CSENDERENDPOINTID']
        return len(senders)

    # returns statistic
    def getColumnDatatypes(self, data):
        return pd.DataFrame(data.dtypes, columns = ['Column Name','Data type'])

    #returns figure
    def getStatistic_1(self, data, title_='Gesamtanzahl Erfolge und Ausfälle'):
        outcome_count = data.groupby('error').count()
        outcome_count = outcome_count.withColumn('error_str',F.when(F.col('error') == 0, 'Erfolg').otherwise('Ausfall'))
        outcome_count_pd = outcome_count.toPandas()
        fig = px.pie(outcome_count_pd, values='count', names='error_str', title=title_ , height=450)
        return fig

    # returns figure
    def getStatistic_2(self, data):
        reversed_percentage = F.udf(lambda avg: 1-avg, FloatType())
        year_outcome_percentage = data.groupby('year').avg('error').orderBy('year', ascending=True)
        year_outcome_percentage = year_outcome_percentage.withColumn('avg_failure',
                                   reversed_percentage(year_outcome_percentage['avg(error)']))
        year_outcome_percentage = year_outcome_percentage.drop("avg(error)")
        year_outcome_percentage_pd = year_outcome_percentage.toPandas()
        fig = px.pie(year_outcome_percentage_pd, values='avg_failure', names='year', title='Anteil Ausfälle pro Jahr', height=450)
        return fig

    # returns figure
    def getStatistic_3(self, data):
        reversed_percentage = F.udf(lambda avg: 1-avg, FloatType())
        month_outcome_percentage = data.groupby('month').avg('error').orderBy('month', ascending=True)
        month_outcome_percentage = month_outcome_percentage.withColumn('avg_failure',
                                   reversed_percentage(month_outcome_percentage['avg(error)']))
        month_outcome_percentage = month_outcome_percentage.drop("avg(error)")
        month_outcome_percentage_pd = month_outcome_percentage.toPandas()
        fig = px.pie(month_outcome_percentage_pd, values='avg_failure', names='month', title='Anteil Ausfälle pro Monat', height=450)
        return fig

    # returns figure
    def getStatistic_4(self, data):
        outcome_status_count_pd = self.getOutcomeStatusCount_pd(data)
        outcome_status_count_pd_1 = outcome_status_count_pd[outcome_status_count_pd.error == 1]
        fig = px.bar(outcome_status_count_pd_1, x='CSTATUS_str', y='count', title='Statushäufigkeit pro Ausfall', labels={
                             "CSTATUS_str": "Status",
                             "count": "Häufigkeit"}, height=500)
        return fig

    # returns figure
    def getStatistic_5(self, data):
        outcome_status_count_pd = self.getOutcomeStatusCount_pd(data)
        outcome_status_count_pd_0 = outcome_status_count_pd[outcome_status_count_pd.error == 0]
        fig = px.bar(outcome_status_count_pd_0, x='CSTATUS_str', y='count', title='Statushäufigkeit pro Erfolg', labels={
                             "CSTATUS_str": "Status",
                             "count": "Häufigkeit"
                         }, height=400)
        return fig

    # returns figure
    def getStatistic_6(self, data):
        service_outcome_count_pd = self.getServiceOutcomeCount_pd(data)
        service_outcome_count_pd_1 = service_outcome_count_pd[service_outcome_count_pd.error == 1]
        fig = px.bar(service_outcome_count_pd_1, x='CSERVICE_str', y='count', title='Servicehäufigkeit pro Ausfall', labels={
                             "CSERVICE_str": "Service",
                             "count": "Häufigkeit"
                         }, height=500)
        return fig

    # returns figure
    def getStatistic_7(self, data):
        service_outcome_count_pd = self.getServiceOutcomeCount_pd(data)
        service_outcome_count_pd_0 = service_outcome_count_pd[service_outcome_count_pd.error == 0]
        fig = px.bar(service_outcome_count_pd_0, x='CSERVICE_str', y='count', title='Servicehäufigkeit pro Erfolg', labels={
                             "CSERVICE_str": "Service",
                             "count": "Häufigkeit"
                         }, height=500)
        return fig

    # returns figure
    def getStatistic_8(self, data):
        year_messagetat_avg = data.groupby('year').avg('CMESSAGETAT2').orderBy('year', ascending=True)
        year_messagetat_avg_pd = year_messagetat_avg.toPandas()
        fig = px.bar(year_messagetat_avg_pd, y='avg(CMESSAGETAT2)', x='year', title='Durchschnittliche Ausführungsdauer pro Jahr', height=450)
        return fig

    # returns figure
    def getStatistic_9(self, data):
        month_messagetat_avg = data.groupby('month').avg('CMESSAGETAT2').orderBy('month', ascending=True)
        month_messagetat_avg_pd = month_messagetat_avg.toPandas()
        fig = px.bar(month_messagetat_avg_pd, y='avg(CMESSAGETAT2)', x='month', title='Durchschnittliche Ausführungsdauer pro Monat', height=450)
        return fig

    # returns figure
    def getStatistic_10(self, data):
        sender_messagetat_avg = data.groupby('CSENDERENDPOINTID').avg('CMESSAGETAT2')
        sender_messagetat_avg = sender_messagetat_avg.sort('avg(CMESSAGETAT2)', ascending=False)
        sender_messagetat_avg_pd = sender_messagetat_avg.limit(5).toPandas()
        fig = px.bar(sender_messagetat_avg_pd, y='avg(CMESSAGETAT2)', x='CSENDERENDPOINTID', title='Top 5 Sender mit der längsten Ausführungsdauer', height=450)
        return fig

    # returns figure
    def getStatistic_11(self, data):
        receiver_messagetat_avg = data.groupby('CRECEIVERENDPOINTID').avg('CMESSAGETAT2')
        receiver_messagetat_avg = receiver_messagetat_avg.sort('avg(CMESSAGETAT2)', ascending=False)
        receiver_messagetat_avg_pd = receiver_messagetat_avg.limit(5).toPandas()
        fig = px.bar(receiver_messagetat_avg_pd, y='avg(CMESSAGETAT2)', x='CRECEIVERENDPOINTID', title='Top 5 Empfänger mit der längsten Ausführungsdauer', height=450)
        return fig

    # returns figure
    def getStatistic_12(self, data):
        service_messagetat_avg = data.groupby('CSERVICE').avg('CMESSAGETAT2')
        inverse_transform_CSERVICE_udf = F.udf(lambda x: enc.inverse_transform(self.npy, x, 'CSERVICE'), StringType())
        service_messagetat_avg = service_messagetat_avg.withColumn("CSERVICE_str", inverse_transform_CSERVICE_udf(service_messagetat_avg.CSERVICE))
        service_messagetat_avg_pd = service_messagetat_avg.toPandas()
        fig = px.bar(service_messagetat_avg_pd, y='avg(CMESSAGETAT2)', x='CSERVICE_str', title='Durchschnittliche Ausführungsdauer pro Service', height=450)
        return fig

    #returns plot
    def visualizeCorrelation(self, data):
        no_str_df = data[self.numeric_columns]
        vector_col = "corr_vars"
        assembler = VectorAssembler(inputCols=no_str_df.columns, outputCol=vector_col)
        df_vector = assembler.transform(no_str_df).select(vector_col)
        corr_matrix = Correlation.corr(df_vector, vector_col).collect()[0][0].toArray().tolist()

        corr_matrix_df = self.getSparkSession().createDataFrame(corr_matrix, no_str_df.columns)

        plot_corr = corr_matrix_df.toPandas()
        plot_corr.index = corr_matrix_df.columns
        return plot_corr

#--------------------------------------------------------------HELPER FUNCTIONS-------------------------------------------------------------------------

    def getSparkSession(self):
        spark = SparkSession.builder.config('spark.local.dir', '/tmp').config("spark.executor.memory", "8g").config("spark.driver.memory", "8g").config("spark.driver.maxResultSize", "0").config("spark.shuffle.registration.maxAttempts", "1").config("spark.task.maxFailures", "1").appName("jupyter").getOrCreate()
        spark.sparkContext.addPyFile("new_message_tracking/FeaturePreparer.py")
        spark.sparkContext.addPyFile("new_message_tracking/DataCollector.py")
        spark.sparkContext.addPyFile("new_message_tracking/Encoder.py")
        return spark

    def getOutcomeStatusCount_pd(self, data):
        outcome_status_count = data.groupby('CSTATUS','error').count().orderBy(['CSTATUS', 'error'], ascending=True)
        inverse_transform_CSTATUS_udf = F.udf(lambda x: enc.inverse_transform(self.npy, x, 'CSTATUS'), StringType())
        outcome_status_count = outcome_status_count.withColumn("CSTATUS_str", inverse_transform_CSTATUS_udf(outcome_status_count.CSTATUS))
        outcome_status_count_pd = outcome_status_count.toPandas()
        return outcome_status_count_pd

    def getServiceOutcomeCount_pd(self, data):
        service_outcome_count = data.groupby('CSERVICE','error').count().orderBy(['CSERVICE', 'error'], ascending=True)
        inverse_transform_CSERVICE_udf = F.udf(lambda x: enc.inverse_transform(self.npy, x, 'CSERVICE'), StringType())
        service_outcome_count = service_outcome_count.withColumn("CSERVICE_str", inverse_transform_CSERVICE_udf(service_outcome_count.CSERVICE))
        service_outcome_count_pd = service_outcome_count.toPandas()
        return service_outcome_count_pd
    
    def printmd(self, string):
        display(Markdown(string))
        
    def visualizeStatisticDataframe(self, df):
        fig = px.bar(df, x=px.Constant('col'), y=df.columns, height=550, color_discrete_sequence=px.colors.qualitative.Bold, labels={
                     "variable": "Merkmal",
                     "value": "Anzahl"
                 })
        fig.update_layout(barmode='group')
        return fig
    
    def zeroCount(self, df):
        if df.count() == 0:
            return 0
        else:
            groupResult = df.groupBy('outcome').count()
            zeroCount = groupResult.filter(groupResult.outcome == 0).first()['count']
            oneCount = groupResult.filter(groupResult.outcome == 1).first()['count']
            return (zeroCount/(zeroCount+oneCount))*100