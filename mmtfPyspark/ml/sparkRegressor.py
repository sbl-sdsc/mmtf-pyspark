#!/usr/bin/env python
'''sparkRegressor.py

Fits a regression model using an MLlib regression method and returns regression
metrics

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from collections import OrderedDict

class SparkRegressor(object):
    '''Fits a regression model using an MLlib regression method and returns
    regression metrics

    Attributes
    ----------
    predictor
       type of multi-class classifier
    label : str
       classification label
    testFraction : float
       test set fraction [0.3]
    seed : int
       random seed
    '''

    def __init__(self, predictor, label, testFraction = 0.3 , seed = 1):

        self.predictor = predictor
        self.label = label
        self.testFraction = testFraction
        self.seed = seed


    def fit(self, data):
        '''Dataset must at least contain the following two columns:
            label : the class labels
            features : feature vector

        Parameters
        ----------
        data : Dataset<Row>

        Returns
        -------
        dict
           mapping of metrics

        '''

        # Split the data into training and test sets (30% held out for testing)
        splits = data.randomSplit([1.0-self.testFraction, self.testFraction], self.seed)
        trainingData = splits[0]
        testData = splits[1]

        # Train a RandomForest model
        self.predictor.setLabelCol(self.label).setFeaturesCol("features")

        # Chain indexer and forest in a Pipeline
        pipeline = Pipeline().setStages([self.predictor])

        # Train Model. This also runs the indexer.
        model = pipeline.fit(trainingData)

        # Make predictions
        predictions = model.transform(testData)

        # Display some sample predictions
        print(f"Sample predictions: {str(self.predictor).split('_')[0]}") # TODO self.predictor.getClass().getSimpleName
        primaryKey = predictions.columns[0]
        predictions.select(primaryKey, self.label, "prediction").sample(False, 0.1, self.seed).show(50)

        # Collect Metrics
        metrics = OrderedDict()
        metrics["Method"] = str(self.predictor).split("_")[0] # TODO

        evaluator = RegressionEvaluator().setLabelCol(self.label) \
                                         .setPredictionCol("prediction") \
                                         .setMetricName("rmse")
        metrics["rmse"] = str(evaluator.evaluate(predictions))

        return metrics
