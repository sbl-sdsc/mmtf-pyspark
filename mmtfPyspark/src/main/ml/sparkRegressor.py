#!/usr/bin/env python
'''

sparkRegressor.py

Authorship information:
    __author__ = "Mars Huang"
    __maintainer__ = "Mars Huang"
    __email__ = "marshuang80@gmail.com:
    __status__ = "Debug"
'''

from pyspark.ml import Pipline
from pyspark.ml.evaluation import RegressionEvaluator

class sparkRegressor(object):
    '''
    '''

    def __init__(self, predictor, label, testFraction = 0.3 , seed = 1):

        self.predictor = predictor
        self.label = label
        self.testFractor = 0.3
        self.seed = 1


    def fit(self, data):
        '''
        Dataset must at least contain the following two columns:
            label : the class labels
            features : feature vector

        Attribute:
            data (Dataset<Row>)
        Return:
            Dictionary with matrics
        '''

        # Split the data into training and test sets (30% held out for testing)
        splits = data.randomSplit([1.0-self.testFraction, self.testFraction], self.seed)
        trainingData = splits[0]
        testData = splits[1]

        # Train a RandomForest model
        predictor.setLabelCol(self.label).setFeaturesCol("features")

        # Chain indexer and forest in a Pipeline
        pipeline = Pipeline().setStages([self.predictor])

        # Train Model. This also runs the indexer.
        model = pipeline.fit(trainingData)

        # Make predictions
        predictions = model.transform(testData)

        # Display some sample predictions
        print(f"Sample predictions: {self.prediction}") # TODO self.predictor.getClass().getSimpleName
        primaryKey = predictions.columns()[0]
        predictions.select(primaryKey, self.label, "prediction").sample(False, 0.1, self.seed).show(50)

        # Collect Metrics
        metrics = {}
        metrics["Method"] = predictor.getClass().getSimpleName() # TODO

        evaluator = RegressionEvaluator().setLabelCol(self.label) \
                                         .setPredictionCol("prediction") \
                                         .setMetricName("rmse")
        metrics["rmse"] = str(evaluator.evaluate(predictions))

        return metrics
