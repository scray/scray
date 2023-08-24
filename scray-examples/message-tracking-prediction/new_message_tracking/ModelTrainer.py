import pandas as pd
import numpy as np
import seaborn as sns

import pyspark.sql.functions as functions
from collections import OrderedDict

import os
os.chdir("/home/jovyan/work")
from xgboost.spark import SparkXGBClassifier, SparkXGBRegressor
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
from IPython.display import display, Markdown
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import PrecisionRecallDisplay
import shutil
import shap
from pyspark.sql.types import StructField, StructType, IntegerType
from pyspark.ml.linalg import VectorUDT

class ModelTrainer():

    path_1 = 'new_message_tracking/data_1/'
    path_2 = 'new_message_tracking/data_2/'
    path_3 = 'new_message_tracking/data_3/'

    model_path_1 = 'new_message_tracking/models/model_1.json'
    model_path_2 = 'new_message_tracking/models/model_2.json'
    model_path_3 = 'new_message_tracking/models/model_3.json'

    train_path_1 = 'new_message_tracking/data_1/train.parquet'
    test_path_1 = 'new_message_tracking/data_1/test.parquet'
    train_path_2 = 'new_message_tracking/data_2/train.parquet'
    test_path_2 = 'new_message_tracking/data_2/test.parquet'
    train_path_3 = 'new_message_tracking/data_3/train.parquet'
    test_path_3 = 'new_message_tracking/data_3/test.parquet'

    train_all = 'new_message_tracking/data_1/all.parquet'

    t1_t2_schema = StructType([
                    StructField("features", VectorUDT(), True),
                    StructField("error", IntegerType(), True)
                ])

    t3_schema = StructType([
                    StructField("features", VectorUDT(), True),
                    StructField("traffic_light", IntegerType(), True)
                ])

    #------------------------------------------------------------------------------------------------- MODEL TRAINING ----------------------------------------------------------------------------------------------------------------------
    
    #-----------------------------------------------------------------------------------
    def perform_training_1(self, data, spark):                                         #
        self.remove_data_from_previous_training(self.path_1)                           #
        self.remove_data_from_previous_training(self.model_path_1)                     #
        model = self.t1_train(data)                                                    #
        test_data=spark.read.parquet(self.test_path_1)                                 #
        prediction = model.transform(test_data)                                        #
        self.printmd('## Plots')                                                       #
        self.e1_get_confusion_matrix(prediction)                                       #
        #self.printmd('## Metrics')                                                     #
        #self.e1_get_metrics(prediction)                                                #
        self.printmd('## Explanations')                                                #
        self.e1_model_explanations(model, test_data)                                   #
        return model
    #-----------------------------------------------------------------------------------

    def t1_train(self,data):
        counter = 0
        n_splits = 4
        count = data.count()
        each_len = int(count/n_splits)

        groupResult = data.groupBy('error').count()
        zeroCount = groupResult.filter(groupResult.error == 0).first()['count']
        oneCount = groupResult.filter(groupResult.error == 1).first()['count']
        print('Percentage of error ' + str((oneCount/(zeroCount+oneCount))*100))

        while counter < n_splits:
            df_py = data.limit(each_len)
            data = data.subtract(df_py)
        
            train_df, test_df = df_py.randomSplit([0.8, 0.2], seed=1)
            train_df.write.mode('append').parquet(self.train_path_1)
            test_df.write.mode('append').parquet(self.test_path_1)
        
            if counter == 0:
                tree = SparkXGBClassifier(
                  features_col="features",
                  label_col="error",
                  num_workers=2,
                )
        
            else:
                tree = tree.load(self.model_path_1)
                
            counter = counter +1
            model = tree.fit(train_df)
            shutil.rmtree(self.model_path_1)
            tree.save(self.model_path_1)

            #if counter == 1:
            #    break
        return model

    def e1_get_metrics(self, prediction):
        evaluatorMulti = MulticlassClassificationEvaluator(labelCol="error", predictionCol="prediction")
        evaluator = BinaryClassificationEvaluator(labelCol="error", rawPredictionCol="prediction", metricName='areaUnderPR')
        
        acc = evaluatorMulti.evaluate(prediction, {evaluatorMulti.metricName: "accuracy"})
        pr = evaluator.evaluate(prediction)
        
        #f1 = evaluatorMulti.evaluate(prediction, {evaluatorMulti.metricName: "f1"})
        #auc = evaluator.evaluate(prediction)

        print("Accuracy: " +str(acc))
        print("Area under Precision-Recall-Curve: " +str(pr))
                                     
        #print("F1: " +str(f1))
        #print("Auc: " +str(auc))

    def e1_get_pr_curve(self, test_data, prediction):
        y_test = test_data.select('error')
        y_test = y_test.toPandas()
        prediction_array = np.array(prediction.select('prediction').collect())
        precision, recall, threshold = precision_recall_curve(y_test, prediction_array)
        plt = PrecisionRecallDisplay(precision, recall).plot()
        display(plt)
        
    def e1_get_confusion_matrix(self, prediction):
        TN = prediction.filter('prediction = 0 AND error = 0').count()
        TP = prediction.filter('prediction = 1 AND error = 1').count()
        FN = prediction.filter('prediction = 0 AND error = 1').count()
        FP = prediction.filter('prediction = 1 AND error = 0').count()

        print('Precision: ' + str(TP/(TP+FP)))
        print('True-Negative-Rate: ' + str(TN/(TN+FP)))

        '''
        cm = [[TN, FP],[FN, TP]]

        cm_matrix = pd.DataFrame(data=cm, columns=['Predict Negative:0', 'Predict Positive:1'],
                         index=['Actual Negative:0', 'Actual Positive:1'])

        display(sns.heatmap(cm_matrix, annot=True, fmt='d', cmap='YlGnBu'))'''

    def e1_model_explanations(self, model, test_data):
        columns = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','year', 'month', 'day', 'hour', 'minute']
        boost = model.get_booster()
        test = test_data.select('features')
        test = test.rdd.map(lambda x:[float(y) for y in x['features']]).toDF(columns)
        
        test_pd =test.toPandas()
        
        shap.initjs()
        explainer = shap.TreeExplainer(boost)
        shap_values = explainer.shap_values(test_pd)

        self.printmd('### Force Plot')
        display(shap.force_plot(explainer.expected_value, shap_values[0,:], test_pd.iloc[0,:]))
        self.printmd('### Beeswarm Plot')
        display(shap.summary_plot(shap_values, test_pd))
        self.printmd('### Bar Plot')
        display(shap.plots.bar(explainer(test_pd)))
        self.printmd('### Waterfall Plot')
        display(shap.plots.waterfall(explainer(test_pd)[0], max_display=14))

    #-----------------------------------------------------------------------------------
    def perform_training_1_presplit(self, train_data, spark):                          #
        self.remove_data_from_previous_training(self.model_path_1)                     #
        model = self.t1_train_presplit(train_data)                                     #
        test_data=spark.read.parquet(self.test_path_1)                                 #
        test_data = spark.createDataFrame(test_data.rdd, self.schema)                  #
        prediction = model.transform(test_data)                                        #
        #self.printmd('## Metrics')                                                     #
        #self.e1_get_metrics(prediction)                                                #
        self.printmd('## Plots')                                                       #
        self.e1_get_confusion_matrix(prediction)                                       #
        #self.e1_get_pr_curve(test_data, prediction)                                    #
    #-----------------------------------------------------------------------------------

    def t1_train_presplit(self,train_data):
        counter = 0
        n_splits = 5
        train_count = train_data.count()
        train_len = int(train_count/n_splits)

        groupResult = train_data.groupBy('error').count()
        zeroCount = groupResult.filter(groupResult.error == 0).first()['count']
        oneCount = groupResult.filter(groupResult.error == 1).first()['count']
        print('Percentage of error ' + str((zeroCount/(zeroCount+oneCount))*100))

        while counter < n_splits:
            train_df = train_data.limit(train_len)
            train_data = train_data.subtract(train_df)
            #train_data.write.mode('overrite').parquet(self.train_all)
        
            if counter == 0:
                tree = SparkXGBClassifier(
                  features_col="features",
                  label_col="error",
                  num_workers=2,
                )
        
            else:
                tree = tree.load(self.model_path_1)
                
            counter = counter +1
            model = tree.fit(train_df)
            shutil.rmtree(self.model_path_1)
            tree.save(self.model_path_1)

            print(counter)
        return model

    #-----------------------------------------------------------------------------------
    def perform_training_1_pca(self, data, spark):                                     #
        self.remove_data_from_previous_training(self.path_1)                           #
        self.remove_data_from_previous_training(self.model_path_1)                     #
        model = self.t1_train(data)                                                    #
        test_data=spark.read.parquet(self.test_path_1)                                 #
        prediction = model.transform(test_data)                                        #
        #self.printmd('## Metrics')                                                     #
        #self.e1_get_metrics(prediction)                                                #
        self.printmd('## Explanations')                                                #
        self.e1_model_explanations_pca(model, test_data)                               #
        self.printmd('## Confusion Matrix')                                            #
        self.e1_get_confusion_matrix(prediction)                                       #
    #-----------------------------------------------------------------------------------
    
    def e1_model_explanations(self, model, test_data):
        columns = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','year', 'month', 'day', 'hour', 'minute']
        boost = model.get_booster()
        test = test_data.select('features')
        test = test.rdd.map(lambda x:[float(y) for y in x['features']]).toDF(columns)
        
        test_pd =test.toPandas()
        
        shap.initjs()
        explainer = shap.TreeExplainer(boost)
        shap_values = explainer.shap_values(test_pd)

        print(shap_values)

        self.printmd('### Force Plot')
        display(shap.force_plot(explainer.expected_value, shap_values[0,:], test_pd.iloc[0,:]))
        self.printmd('### Beeswarm Plot')
        display(shap.summary_plot(shap_values, test_pd))
        self.printmd('### Bar Plot')
        display(shap.plots.bar(explainer(test_pd)))
        self.printmd('### Waterfall Plot')
        display(shap.plots.waterfall(explainer(test_pd)[0], max_display=14))
        
    #-------------------------------------------------------------------------------
    def perform_training_2(self, data, spark):                                     #
        self.remove_data_from_previous_training(self.path_2)                       #
        #self.remove_data_from_previous_training(self.model_path_2)                 #
        model = self.t2_train(data)                                                #
        test_data=spark.read.parquet(self.test_path_2)                             #
        prediction = model.transform(test_data)                                    #
        self.printmd('## Metrics')                                                 #
        self.e2_get_metrics(prediction)                                            #
        self.printmd('## Explanations')                                            #
        self.e2_model_explanations(model, test_data)                               #
    #-------------------------------------------------------------------------------

    def t2_train(self,data):
        counter = 1
        n_splits = 4
        count = data.count()
        each_len = int(count/n_splits)

        while counter < n_splits:
            df_py = data.limit(each_len)
            data = data.subtract(df_py)
        
            train_df, test_df = df_py.randomSplit([0.8, 0.2], seed=1)
            train_df.write.mode('append').parquet(self.train_path_2)
            test_df.write.mode('append').parquet(self.test_path_2)

            if counter == 1:
                tree = SparkXGBRegressor(
                  features_col="features",
                  label_col="CMESSAGETAT2",
                  num_workers=2,
                )

            #else:
            tree = tree.load(self.model_path_2)
                
            counter = counter +1
            model = tree.fit(train_df)
            shutil.rmtree(self.model_path_2)
            tree.save(self.model_path_2)
        return model

    def e2_get_metrics(self, prediction):
        evaluator = RegressionEvaluator(labelCol="CMESSAGETAT2")
        evaluator.setPredictionCol("prediction")
        rmse = evaluator.evaluate(prediction, {evaluator.metricName: "rmse"})
        print("RMSE: " +str(rmse))

    def e2_model_explanations(self, model, test_data):
        columns = ['CSERVICE','CSENDERENDPOINTID','CSENDERPROTOCOL','CINBOUNDSIZE','year', 'month', 'day', 'hour', 'minute']
        boost = model.get_booster()
        test = test_data.select('features')
        test = test.rdd.map(lambda x:[float(y) for y in x['features']]).toDF(columns)
        
        test_pd =test.toPandas()
        
        shap.initjs()
        explainer = shap.TreeExplainer(boost)
        shap_values = explainer.shap_values(test_pd)

        self.printmd('### Force Plot')
        display(shap.force_plot(explainer.expected_value, shap_values[0,:], test_pd.iloc[0,:]))
        self.printmd('### Beeswarm Plot')
        display(shap.summary_plot(shap_values, test_pd))
        self.printmd('### Bar Plot')
        display(shap.plots.bar(explainer(test_pd)))
        self.printmd('### Waterfall Plot')
        display(shap.plots.waterfall(explainer(test_pd)[0], max_display=14))
    
    #-------------------------------------------------------------------------------
    def perform_training_3(self, data, spark):                                     #
        self.remove_data_from_previous_training(self.path_3)                       #
        self.remove_data_from_previous_training(self.model_path_3)                 #
        model = self.t3_train(data)                                                #
        test_data=spark.read.parquet(self.test_path_3)                             #
        prediction = model.transform(test_data)                                    #
        #self.printmd('## Metrics')                                                 #
        #self.e3_get_metrics(prediction)                                            #
        self.printmd('## Explanations')                                            #
        self.e3_model_explanations(model, test_data)                               #
        self.printmd('## Confusion Matrix')                                        #
        self.e3_get_confusion_matrix(prediction)                                   #
    #-------------------------------------------------------------------------------

    def t3_train(self,data):
        counter = 1
        n_splits = 3
        count = data.count()
        each_len = int(count/n_splits)

        while counter < n_splits:
            df_py = data.limit(each_len)
            data = data.subtract(df_py)
        
            train_df = df_py.sample(0.8, seed=1)
            test_df= df_py.subtract(train_df)
            train_df.write.mode('append').parquet(self.train_path_3)
            test_df.write.mode('append').parquet(self.test_path_3)
        
            if counter == 1:
                tree = SparkXGBClassifier(
                  features_col="features",
                  label_col="traffic_light",
                  num_workers=2,
                )
        
            else:
                tree = tree.load(self.model_path_3)
                
            counter = counter +1
            model = tree.fit(train_df)
            shutil.rmtree(self.model_path_3)
            tree.save(self.model_path_3)
        return model

    def e3_get_metrics(self, prediction):
        evaluatorMulti = MulticlassClassificationEvaluator(labelCol="traffic_light", predictionCol="prediction")
        
        acc = evaluatorMulti.evaluate(prediction, {evaluatorMulti.metricName: "accuracy"})

        print("Accuracy: " +str(acc))

    def e3_get_confusion_matrix(self, prediction):
        prediction.groupBy('traffic_light', 'prediction').count()
        
        TN = prediction.filter('prediction = 0 AND traffic_light = 0').count()
        TP = prediction.filter('prediction = 1 AND traffic_light = 1').count()
        FN = prediction.filter('prediction = 0 AND traffic_light = 1').count()
        FP = prediction.filter('prediction = 1 AND traffic_light = 0').count()

        print('Precision: ' + str(TP/(TP+FP)))
        print('True-Negative-Rate: ' + str(TN/(TN+FP)))
        

    def e3_model_explanations(self, model, test_data):
        columns = ['CSERVICE','CSERVICE_mode','CSENDERENDPOINTID_mode','CSENDERPROTOCOL_mode','CINBOUNDSIZE_avg','year_mode','month_mode','day_mode', 'count']
        boost = model.get_booster()
        test = test_data.select('features')
        test = test.rdd.map(lambda x:[float(y) for y in x['features']]).toDF(columns)
        
        test_pd =test.toPandas()
        
        shap.initjs()
        explainer = shap.TreeExplainer(boost)
        shap_values = explainer.shap_values(test_pd)

        self.printmd('### Force Plot')
        display(shap.force_plot(explainer.expected_value, shap_values[0,:], test_pd.iloc[0,:]))
        self.printmd('### Beeswarm Plot')
        display(shap.summary_plot(shap_values, test_pd))
        self.printmd('### Bar Plot')
        display(shap.plots.bar(explainer(test_pd)))
        self.printmd('### Waterfall Plot')
        display(shap.plots.waterfall(explainer(test_pd)[0], max_display=14))


    #------------------------------------------------------------------------------------------------- HELPER FUNCTIONS ---------------------------------------------------------------------------------------------------------------------

    def remove_data_from_previous_training(self, path):
        shutil.rmtree(path)
        os.mkdir(path)

    def evaluate_binary(self, test_data, model):
        predictionAndLabels = test_data.map(lambda lp: (float(model.transform(lp.select('features'))), lp.error))
        metrics = BinaryClassificationMetrics(predictionAndLabels)
        print("Area under PR = %s" % metrics.areaUnderPR)
        print("Area under ROC = %s" % metrics.areaUnderROC)

    def printmd(self, string):
        display(Markdown(string))
        