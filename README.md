# Mmtf-pyspark
Methods for parallel and distributed analysis and mining of the Protein Data Bank using MMTF and Apache Spark.

# Requirements

## anaconda
We strongly recommend that you have [anaconda](https://docs.continuum.io/anaconda/install/) and the lastest version of python installed


## biopython
Please install [biopython](http://biopython.org/wiki/Download)

## mmtf-python

The most up-to-date mmtf-python is not on pip or Anaconda, please clone it from the following [github link](https://github.com/rcsb/mmtf-python)


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

### Windows

0.  If you already have anaconda and GOW installed, skip to step 2.

1.  Using GOW(recommended):

    Basically, GOW allows you to use linux commands on windows. In this install, we will need curl, gzip, tar which GOW provides.

    [Download GOW](https://github.com/bmatzelle/gow/releases/download/v0.8.0/Gow-0.8.0.exe)

2.  Go to the Apache Spark website [link](http://spark.apache.org/downloads.html)

    a) Choose a Spark release

    b) Choose a package type

    c) Choose a download type: (Direct Download)

    d) Download Spark

    e) Move the file to where you want to unzip it

    f) Unzip the file. Use the commands below

    ```
    gzip -d YOUR_SPARK_FILENAME.tgz

    tar xvf YOUR_SPARK_FILENAME.tar
    ```
    
3.  Download winutils.exe into "YOUR_DIRECTORY\YOUR_SPARK_VERSION\bin" using following command.
	As an example in following steps, "YOUR_DIRECTORY" could be "C:\opt\spark", "YOUR_SPARK_VERSION" could be "spark-2.2.0-bin-hadoop2.6".
	
```
curl -k -L -o winutils.exe https://github.com/steveloughran/winutils/blob/master/hadoop-2.6.0/bin/winutils.exe?raw=true
```

4.  Make sure you have Java 7+ installed on your machine.

5.  Next, we will edit our environmental variables so we can open a spark notebook in any directory.

    (1) Find environmental variables:

    a) In Search, search for and then select: System (Control Panel)

    b) Click the Advanced system settings link.

    c) Click Environment Variables.

    d) In the section System Variables find the environment variables and select it. Click Edit. If the environment variable does not exist, click New.

    (2) Set environmental variables:

    a) set "SPARK_HOME" to "YOUR_DIRECTORY\YOUR_SPARK_VERSION"

    b) set "HADOOP_HOME" to "YOUR_DIRECTORY\YOUR_SPARK_VERSION"

    c) set "PYSPARK_DRIVER_PYTHON" to "ipython"

    d) set "PYSPARK_DRIVER_PYTHON_OPTS" to "notebook"

    e) Add ";YOUR_DIRECTORY\YOUR_SPARK_VERSION\bin" to your "PATH".

6.  Close your terminal and open a new one. Type the command below to test if pyspark has been installed.

```
pyspark --master local[2]
```

### Linux [link](https://medium.com/@GalarnykMichael/install-spark-on-ubuntu-pyspark-231c45677de0)

## JDK

Depending on your OS, Java Development Kit might need to be installed


# Usage

You can either create a scipt/jupyter-notebook in the *mmtf-pyspark/mmtfPyspark* and follow the examples from *mmtf-pyspark/mmtfPyspark/DataAnalysisWithDataFrameExample*.

Or you can import mmtf-pyspark to your code from another directory with the following lines:

```python
import sys
sys.path.append("<path to your mmtf-pyspark folder")
sys.path.append("<path to your mmtf-python folder")
import mmtfPyspark
from mmtfPyspark.src.main import filters
```

# Examples

Filters usage examples can be found in *mmtf-pyspark/mmtfPyspark/src/demos/filters*

# Hadoop Sequence Files

The Hadoop sequence files can be downloaded with:
```
curl -O http://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar
tar -xvf full.tar
```
