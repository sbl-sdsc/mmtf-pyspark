# Mmtf-pyspark
Methods for parallel and distributed analysis and mining of the Protein Data Bank using MMTF and Apache Spark.

# Requirements

## anaconda
We strongly recommend that you have [anaconda](https://docs.continuum.io/anaconda/install/) and the lastest version of python installed


## mmtf-python 

The most up-to-date mmtf-python is not on pip or Anaconda, please clone it from the following [github link](https://github.com/rcsb/mmtf-python)

To use the package, please edit the file *mmtf-pyspark/mmtf_pyspark/src/main/__init__.py* and add the path to your local mmtf-python

## pyspark

The following links are tutorials to install pyspark on differnt OS systems

### Mac
   
Using Homebrew(recommended):

1. Install Homebrew, go to terminal and run

```
/usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
```

2. Install apache-spark, run

```
brew install apache-spark
```

3. Install py4j, run 

```
conda install py4j
```
[Manual download](https://medium.com/@GalarnykMichael/install-spark-on-mac-pyspark-453f395f240b) 

### [Windows](https://medium.com/@GalarnykMichael/install-spark-on-windows-pyspark-4498a5d8d66c)

### [Linux](https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0)


# Usage

You can either create a scipt/jupyter-notebook in the *mmtf-pyspark/mmtfPyspark* and follow the examples from *mmtf-pyspark/mmtfPyspark/DataAnalysisWithDataFrameExample*. 

Or you can import mmtf-pyspark to your code from another directory with the following lines:

```python
import sys
sys.path.append("<path to your mmtf-pyspark folder")
import mmtfPyspark
from mmtfPyspark.src.main import filters
``` 

# Examples

Filters usage examples can be found in *mmtf-pyspark/mmtfPyspark/src/demos/filters* 
