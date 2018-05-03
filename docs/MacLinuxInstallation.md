# Installation on MacOS and Linux

### Install Java SE Development Toolkit (JDK 1.8)
To check if you have JDK installed, type the following on your terminal:
```
javac -version
```

If you do not have JDK, or if you are using any version other than 1.8, please install JDK 1.8.

[Download JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) and run the installer.


### Install Git
The Git version control system is used to download respositories from Github.

To check if you have git installed, type the following line on your terminal:
```
git --version
```

[Download and install Git](https://git-scm.com/downloads)


### Install Spark
To install and setup Apache Spark 2.3 and Hadoop 2.7, run the following commands on your terminal:
```
curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/install_spark.sh -o install_spark.sh
. ./install_spark.sh
```


### Install mmtfPyspark
To install mmtfPyspark, make sure you have pip:

```
pip --version
```

To install pip on your mac:
```
sudo easy_install pip
```

For linux machines, please visit the following website:

[Install pip on linux](https://packaging.python.org/guides/installing-using-linux-tools/)

mmtfPyspark can be installed using two different ways:
 * PyPI install (from the python packaging index):

   ```
   pip install mmtfPyspark
   ```

 * pip install (cloning github repository and do a local installation):

   ```
   git clone https://github.com/sbl-sdsc/mmtf-pyspark.git
   pip install ./mmtf-pyspark/
   ```

If there are any errors installing package, try upgrading your pip by:

   ```
   pip install --upgrade pip    
   ```

By cloning the Github repository using the pip install method, sample jupyter notebooks and tutorials can be found in the *mmtf-pyspark/demos* directory.

If there are any errors installing package, try upgrading your pip by:

   ```
   pip install --upgrade pip    
   ```


### Testing installation
To test if the installation is successful:

```
curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/test_mmtfPyspark.py -o test_mmtfPyspark.py

python test_mmtfPyspark.py
```

If the metadata of 1AQ1 is printed, you have successfully intalled mmtfPyspark.


### [OPTIONAL] Hadoop Sequence Files
Hadoop sequence files of all PDB structures can be downloaded and environmental variables can be set by running the following command:
```
curl https://raw.githubusercontent.com/sbl-sdsc/mmtf-pyspark/master/bin/download_mmtf_files.sh -o download_mmtf_files.sh
. ./download_mmtf_files.sh
```
