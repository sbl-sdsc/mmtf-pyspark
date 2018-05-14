#!/usr/bin/env python
'''sparkMultiClassClassifier.py

Fits a multi-class classification model using mllib classification method and
returns classification metrics.

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml import Pipeline
from pyspark.mllib.evaluation import BinaryClassificationMetrics, MulticlassMetrics
from collections import OrderedDict
import time


class SparkMultiClassClassifier(object):
    '''Fits a multi-class classification model using mllib classification method and
    returns classification metrics.

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

    def __init__(self, predictor, label, testFraction=0.3, seed=1):

        self.predictor = predictor
        self.label = label
        self.testFraction = testFraction
        self.seed = seed

    def fit(self, data):
        '''Dataset must at least contain the following two columns:
        label: the class labels
        features: feature vector

        Parameters
        ----------
        data : Dataset<Row>
           input data

        Returns
        -------
        dict
           map with metrics
        '''

        start = time.time()

        classCount = int(data.select(self.label).distinct().count())

        labelIndexer = StringIndexer().setInputCol(self.label) \
                                      .setOutputCol("indexedLabel") \
                                      .fit(data)

        # Split the data into training and test sets (30% held out for testing)
        splits = data.randomSplit(
            [1.0 - self.testFraction, self.testFraction], self.seed)
        trainingData = splits[0]
        testData = splits[1]

        labels = labelIndexer.labels


        print("\n Class\tTrain\tTest")
        for l in labels:
            print("%s\t%i\t%i" % (l \
                                  ,(trainingData.filter(trainingData[self.label] == l)).count() \
                                  ,(testData.filter(testData[self.label] == l)).count() \
                                  )
                  )

        # Set input columns
        self.predictor.setLabelCol("indexedLabel").setFeaturesCol("features")

        # Convert indexed labels back to original labels
        labelConverter = IndexToString().setInputCol("prediction") \
                                        .setOutputCol("predictedLabel") \
                                        .setLabels(labelIndexer.labels)

        # Chain indexers and forest ina Pipline
        pipeline = Pipeline().setStages([labelIndexer, self.predictor, labelConverter])

        # Train model. This also runs the indexers
        model = pipeline.fit(trainingData)

        # Make predictions
        predictions = model.transform(testData).cache()

        # Display some sample predictions
        print(f"\nSample predictions: {str(self.predictor).split('_')[0]}") # TODO predictor.getClass().getSimpleName()
        predictions.sample(False, 0.1, self.seed).show(5)

        predictions = predictions.withColumnRenamed(self.label, "stringLabel")
        predictions = predictions.withColumnRenamed("indexedLabel", self.label)

        # Collect metrics

        pred = predictions.select("prediction", self.label)

        metrics = OrderedDict()
        metrics["Method"] = str(self.predictor).split('_')[0]

        if classCount == 2:
            b = BinaryClassificationMetrics(pred.rdd)
            metrics["AUC"] = str(b.areaUnderROC)
        m = MulticlassMetrics(pred.rdd)
        metrics["F"] = str(m.weightedFMeasure())
        metrics["Accuracy"] = str(m.accuracy)
        metrics["Precision"] = str(m.weightedPrecision)
        metrics["Recall"] = str(m.weightedRecall)
        metrics["False Positive Rate"] = str(m.weightedFalsePositiveRate)
        metrics["True Positive Rate"] = str(m.weightedTruePositiveRate)
        metrics[""] = f"\nConfusion Matrix\n{labels}\n{m.confusionMatrix()}"

        end = time.time()
        print(f"Total time taken: {end-start}\n")

        return metrics
