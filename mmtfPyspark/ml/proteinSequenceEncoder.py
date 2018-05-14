'''proteinSequenceEncoder.py

This class encodes a protein sequence into a feature vector.
The protein sequence must be present in the input data set, the default column
name is "sequence". The default column name for the feature vector is "features".

'''
__author__ = "Mars (Shih-Cheng) Huang"
__maintainer__ = "Mars (Shih-Cheng) Huang"
__email__ = "marshuang80@gmail.com"
__version__ = "0.2.0"
__status__ = "Done"

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.linalg import VectorUDT
from pyspark.ml.feature import Word2Vec, Word2VecModel
from mmtfPyspark.ml import sequenceNgrammer


class ProteinSequenceEncoder(object):
    '''
    This class encodes a protein sequence into a feature vector.
    The protein sequence must be present in the input data set,
    the default column name is "sequence". The default column name
    for the feature vector is "features".

    Attributes
    ----------
    data : DataFrame
       input data to be encoded [None]
    inputCol : str
       name of the input column [sequence]
    outputCol : str
       name of the output column [features]
    '''

    model = None

    AMINO_ACIDS21 = ['A', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'K', 'L', 'M', \
                     'N', 'P', 'Q', 'R', 'S', 'T', 'V', 'W', 'X', 'Y']

    properties = {
        'A' : [1.28,0.05,1.00,0.31,6.11,0.42,0.23],
        'G' : [0.00,0.00,0.00,0.00,6.07,0.13,0.15],
        'V' : [3.67,0.14,3.00,1.22,6.02,0.27,0.49],
        'L' : [2.59,0.19,4.00,1.70,6.04,0.39,0.31],
        'I' : [4.19,0.19,4.00,1.80,6.04,0.30,0.45],
        'F' : [2.94,0.29,5.89,1.79,5.67,0.30,0.38],
        'Y' : [2.94,0.30,6.47,0.96,5.66,0.25,0.41],
        'W' : [3.21,0.41,8.08,2.25,5.94,0.32,0.42],
        'T' : [3.03,0.11,2.60,0.26,5.60,0.21,0.36],
        'S' : [1.31,0.06,1.60,-0.04,5.70,0.20,0.28],
        'R' : [2.34,0.29,6.13,-1.01,10.74,0.36,0.25],
        'K' : [1.89,0.22,4.77,-0.99,9.99,0.32,0.27],
        'H' : [2.99,0.23,4.66,0.13,7.69,0.27,0.30],
        'D' : [1.60,0.11,2.78,-0.77,2.95,0.25,0.20],
        'E' : [1.56,0.15,3.78,-0.64,3.09,0.42,0.21],
        'N' : [1.60,0.13,2.95,-0.60,6.52,0.21,0.22],
        'Q' : [1.56,0.18,3.95,-0.22,5.65,0.36,0.25],
        'M' : [2.35,0.22,4.43,1.23,5.71,0.38,0.32],
        'P' : [2.67,0.00,2.72,0.72,6.80,0.13,0.34],
        'C' : [1.77,0.13,2.43,1.54,6.35,0.17,0.41],
        'X' : [0.00,0.00,0.00,0.00,0.00,0.00,0.00],
    }

	# Source: https://ftp.ncbi.nih.gov/repository/blocks/unix/blosum/BLOSUM/blosum62.blast.new
    blosum62 = {
         #      A  R  N  D  C  Q  E  G  H  I  L  K  M  F  P  S  T  W  Y  V
        'A' : [ 4,-1,-2,-2, 0,-1,-1, 0,-2,-1,-1,-1,-1,-2,-1, 1, 0,-3,-2, 0],
        'R' : [-1, 5, 0,-2,-3, 1, 0,-2, 0,-3,-2, 2,-1,-3,-2,-1,-1,-3,-2,-3],
        'N' : [-2, 0, 6, 1,-3, 0, 0, 0, 1,-3,-3, 0,-2,-3,-2, 1, 0,-4,-2,-3],
        'D' : [-2,-2, 1, 6,-3, 0, 2,-1,-1,-3,-4,-1,-3,-3,-1, 0,-1,-4,-3,-3],
        'C' : [ 0,-3,-3,-3, 9,-3,-4,-3,-3,-1,-1,-3,-1,-2,-3,-1,-1,-2,-2,-1],
        'Q' : [-1, 1, 0, 0,-3, 5, 2,-2, 0,-3,-2, 1, 0,-3,-1, 0,-1,-2,-1,-2],
        'E' : [-1, 0, 0, 2,-4, 2, 5,-2, 0,-3,-3, 1,-2,-3,-1, 0,-1,-3,-2,-2],
        'G' : [ 0,-2, 0,-1,-3,-2,-2, 6,-2,-4,-4,-2,-3,-3,-2, 0,-2,-2,-3,-3],
        'H' : [-2, 0, 1,-1,-3, 0, 0,-2, 8,-3,-3,-1,-2,-1,-2,-1,-2,-2, 2,-3],
        'I' : [-1,-3,-3,-3,-1,-3,-3,-4,-3, 4, 2,-3, 1, 0,-3,-2,-1,-3,-1, 3],
        'L' : [-1,-2,-3,-4,-1,-2,-3,-4,-3, 2, 4,-2, 2, 0,-3,-2,-1,-2,-1, 1],
        'K' : [-1, 2, 0,-1,-3, 1, 1,-2,-1,-3,-2, 5,-1,-3,-1, 0,-1,-3,-2,-2],
        'M' : [-1,-1,-2,-3,-1, 0,-2,-3,-2, 1, 2,-1, 5, 0,-2,-1,-1,-1,-1, 1],
        'F' : [-2,-3,-3,-3,-2,-3,-3,-3,-1, 0, 0,-3, 0, 6,-4,-2,-2, 1, 3,-1],
        'P' : [-1,-2,-2,-1,-3,-1,-1,-2,-2,-3,-3,-1,-2,-4, 7,-1,-1,-4,-3,-2],
        'S' : [ 1,-1, 1, 0,-1, 0, 0, 0,-1,-2,-2, 0,-1,-2,-1, 4, 1,-3,-2,-2],
        'T' : [ 0,-1, 0,-1,-1,-1,-1,-2,-2,-1,-1,-1,-1,-2,-1, 1, 5,-2,-2, 0],
        'W' : [-3,-3,-4,-4,-2,-2,-3,-2,-2,-3,-2,-3,-1, 1,-4,-3,-2,11, 2,-3],
        'Y' : [-2,-2,-2,-3,-2,-1,-2,-3, 2,-1,-1,-2,-1, 3,-3,-2,-2, 2, 7,-1],
        'V' : [ 0,-3,-3,-3,-1,-2,-2,-3,-3, 3, 1,-2, 1,-1,-2,-2, 0,-3,-1, 4],
        'X' : [-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4,-4],
    }


    def __init__(self, data = None, inputCol = "sequence", outputCol = "features"):

        self.data = data
        self.inputCol = inputCol
        self.outputCol = outputCol


    def one_hot_encode(self, data = None, inputCol = None, outputCol = None):
        '''
        One-hot encodes a protein sequence. The one-hot encoding
        encodes the 20 natural amino acids, plus X for any other
        residue for a total of 21 elements per residue.

        Parameters
        ----------
        data : DataFrame
           input data to be encoded [None]
        inputCol : str
           name of the input column [None]
        outputCol : str
           name of the output column [None]
        '''

        # Setting class variables
        if data is not None:
            self.data = data

        if inputCol is not None:
            self.inputCol = inputCol

        if outputCol is not None:
            self.outputCol = outputCol

        if self.data is None:
            raise ValueError("Class variable data is not defined, please pass\
                             in a dataframe into the data parameter")

        session = SparkSession.builder.getOrCreate()
        AMINO_ACIDS21 = self.AMINO_ACIDS21

        # Encoder function to be passed as User Defined Function (UDF)
        def _encoder(s):

            values = [0] * len(AMINO_ACIDS21) * len(s)

            for i in range(len(s)):

                if s[i] in AMINO_ACIDS21:
                    index = AMINO_ACIDS21.index(s[i])

                else:
                    index = AMINO_ACIDS21.index('X')

                values[i*len(AMINO_ACIDS21) + index] = 1

            return Vectors.dense(values)

        session.udf.register("encoder", _encoder, VectorUDT())

        self.data.createOrReplaceTempView("table")
        sql = f"SELECT *, encoder({self.inputCol}) AS {self.outputCol} from table"

        data = session.sql(sql)

        return data


    def property_encode(self, data = None, inputCol = None, outputCol = None):
        '''Encodes a protein sequence by 7 physicochemical properties

        References
        ----------
        Meiler, J., Müller, M., Zeidler, A. et al. J Mol Model (2001)
        https://link.springer.com/article/10.1007/s008940100038

        Parameters
        ----------
        data : DataFrame
           input data to be encoded [None]
        inputCol : str
           name of the input column [None]
        outputCol : str
           name of the output column [None]

        Returns
        -------
        dataset
           dataset with feature vector appended
        '''

        # Setting class variables
        if data is not None:
            self.data = data

        if inputCol is not None:
            self.inputCol = inputCol

        if outputCol is not None:
            self.outputCol = outputCol

        if self.data is None:
            raise ValueError("Class variable data is not defined, please pass\
                             in a dataframe into the data parameter")

        session = SparkSession.builder.getOrCreate()
        properties = self.properties

        #Encoder function to be passed as User Defined Function (UDF)
        def _encoder(s):
            values = []

            for i in range(len(s)):

                if s[i] in properties:
                    values += properties[s[i]]

            return Vectors.dense(values)

        session.udf.register("encoder", _encoder, VectorUDT())

        self.data.createOrReplaceTempView("table")
        sql = f"SELECT *, encoder({self.inputCol}) AS {self.outputCol} from table"

        data = session.sql(sql)

        return data


    def blosum62_encode(self, data = None, inputCol = None, outputCol = None):
        '''Encodes a protein sequence by 7 Blosum62

        References
        ----------
        Blosum Matrix
        https://ftp.ncbi.nih.gov/repository/blocks/unix/blosum/BLOSUM/blosum62.blast.new

        Parameters
        ----------
        data : DataFrame
           input data to be encoded [None]
        inputCol : str
           name of the input column [None]
        outputCol : str
           name of the output column [None]

        Returns
        -------
        dataset
           dataset with feature vector appended
        '''

        if data is not None:
            self.data = data

        if inputCol is not None:
            self.inputCol = inputCol

        if outputCol is not None:
            self.outputCol = outputCol

        if self.data is None:
            raise ValueError("Class variable data is not defined, please pass\
                             in a dataframe into the data parameter")

        session = SparkSession.builder.getOrCreate()
        blosum62 = self.blosum62

        #Encoder function to be passed as User Defined Function (UDF)
        def _encoder(s):
            values = []

            for i in range(len(s)):

                if s[i] in blosum62:
                    values += blosum62[s[i]]

            return Vectors.dense(values)

        session.udf.register("encoder", _encoder, VectorUDT())

        self.data.createOrReplaceTempView("table")
        sql = f"SELECT *, encoder({self.inputCol}) AS {self.outputCol} from table"

        data = session.sql(sql)

        return data


    def overlapping_ngram_word2vec_encode(self, data = None, inputCol = None,
                                          outputCol = None, n = None,
                                          windowSize = None, vectorSize = None,
                                          fileName = None, sc = None):
        '''Encodes a protein sequence by converting it into n-grams and
        then transforming it into a Word2Vec feature vector.

        If given word2Vec file name, then this function encodes a protein
        sequence by converting it into n-grams and then transforming it using
        pre-trained word2Vec model read from that file

        Parameters
        ----------
        data : DataFrame
           input data to be encoded [None]
        inputCol : str
           name of the input column [None]
        outputCol : str
           name of the output column [None]
        n : int
           The number of words in an n-gram [None]
        windowSize : int
           width of the window used to slide across the  squence, context words from -window to window  [None]
        vectorSize :int
           dimension of the feature vector [None]
        fileName : str
           filename of Word2Vec model [None]

        Returns
        -------
        dataset
           dataset with features vector added to original dataset
        '''

        if data is not None:
            self.data = data

        if inputCol is not None:
            self.inputCol = inputCol

        if outputCol is not None:
            self.outputCol = outputCol

        if self.data is None:
            raise ValueError("Class variable data is not defined, please pass\
                             in a dataframe into the data parameter")

        # Create n-grams out of the sequence
        # E.g., 2-gram IDCGH, ... =>[ID, DC, CG, GH, ...]

        data = sequenceNgrammer.ngram(self.data, n, "ngram")

        if not (n == None and windowSize == None and vectorSize == None):
            # Convert n-grams to W2V freature vector
            # [ID, DC, CG, GH, ...] => [0.1234, 0.2394, ...]
            word2Vec = Word2Vec()
            word2Vec.setInputCol("ngram") \
                    .setOutputCol(self.outputCol) \
                    .setNumPartitions(8) \
                    .setWindowSize(windowSize) \
                    .setVectorSize(vectorSize) \

            self.model = word2Vec.fit(data)

        elif fileName != None and sc != None:
            reader = Word2VecModel()

            self.model = reader.load(sc, fileName)

            print(f"model file : {fileName} \n \
                    inputCol : {self.model.getInputCol()} \n \
                    windowSize : {self.model.getWindowSize()} \n \
                    vectorSize : {self.model.getVectorSize()}")

            self.model.setOutputCol(self.outputCol)

        else:
            raise Exception("Either provide word2Vec file (filename) + SparkContext (sc), \
                            or number of words(n) + window size(windowSize) \
                            + vector size (vetorSize), for function parameters")
            return

        return self.model.transform(data)


    def shifted_3gram_word2vec_encode(self, data = None, inputCol = None,
                                      outputCol = None, windowSize = None,
                                      vectorSize = None, fileName = None,
                                      sc = None):
        '''Encodes a protein sequence as three non-overlapping 3-grams,
        trains a Word2Vec model on the 3-grams, and then averages the
        three resulting freature vectors.


        Parameters
        ----------
        data : DataFrame
           input data to be encoded [None]
        inputCol : str
           name of the input column [None]
        outputCol : str
           name of the output column [None]
        windowSize : int
           width of the window used to slide across the sequence context words from -window to window 
        vectorSize : int
           dimension of the feature vector [None]
        fileName : str
           filename of Word2VecModel [None]
        sc : SparkContext
           spark context [None]

        Returns
        -------
        dataset
           dataset with features vector added to original dataset

        References
        ----------
        Asgari E, Mofrad MRK (2015) Continuous Distributed Representation of Biological Sequences for Deep Proteomics and Genomics.  PLOS ONE 10(11): e0141287. doi: https://doi.org/10.1371/journal.pone.0141287
        '''

        if data is not None:
            self.data = data

        if inputCol is not None:
            self.inputCol = inputCol

        if outputCol is not None:
            self.outputCol = outputCol

        if self.data is None:
            raise ValueError("Class variable data is not defined, please pass\
                             in a dataframe into the data parameter")

        # Create n-grams out of the sequence
        # e.g., 2-gram [IDCGH, ...] => [ID. DC, CG, GH,...]

        data = sequenceNgrammer.shifted_ngram(self.data, 3, 0, "ngram0")
        data = sequenceNgrammer.shifted_ngram(data, 3, 1, "ngram1")
        data = sequenceNgrammer.shifted_ngram(data, 3, 2, "ngram2")
        if not (windowSize == None and vectorSize == None):

            ngram0 = data.select("ngram0").withColumnRenamed("ngram0","ngram")
            ngram1 = data.select("ngram1").withColumnRenamed("ngram1","ngram")
            ngram2 = data.select("ngram2").withColumnRenamed("ngram2","ngram")

            ngrams = ngram0.union(ngram1).union(ngram2)

            # Convert n-grams to W2V feature vector
            # [I D, D C, C G, G H, ... ] => [0.1234, 0.2394, .. ]
            word2Vec = Word2Vec()

            word2Vec.setInputCol("ngram") \
                    .setOutputCol("feature") \
                    .setMinCount(10) \
                    .setNumPartitions(8) \
                    .setWindowSize(windowSize) \
                    .setVectorSize(vectorSize)

            self.model = word2Vec.fit(ngrams)

        elif fileName != None and sc != None:
            reader = Word2VecModel()

            self.model = reader.load(sc, fileName)

            print(f"model file : {fileName} \n \
                    inputCol : {self.model.getInputCol()} \n \
                    windowSize : {self.model.getWindowSize()} \n \
                    vectorSize : {self.model.getVectorSize()}")

        else:
            raise Exception("Either provide word2Vec file (filename) + SparkContext (sc), \
                            or window size(windowSize) + vector size (vetorSize), \
                            for function parameters")
            return

        #data = data.withColumn("feature0",self.model.transform(data.select('ngram0').withColumnRenamed("ngram0","ngram")))
        for i in reversed(range(3)):
            feature = self.model.transform(data.select('ngram' + str(i)).withColumnRenamed("ngram" + str(i),"ngram"))
            data = data.join(feature.withColumnRenamed("ngram","ngram" + str(i)), "ngram" + str(i))
            data = data.withColumnRenamed("feature", "feature" + str(i))


        data = self._average_feature_vectors(data, self.outputCol)
        data.printSchema()

        cols = ['structureChainId','sequence','labelQ8','labelQ3','ngram0','ngram1',\
                'ngram2','feature0','feature1','feature2', 'features']

        data = data.select(cols)


        return data


    def get_word2vec_model(self):
        '''Returns a Word2VecModel created by overlapping_ngram_word2vec_encode()

        Returns
        -------
        model
           overlapping Ngram Word2VecModel if available, otherwise None
        '''

        return self.model


    def _average_feature_vectors(self, data, outputCol):
        '''Average the feature vectors

        Parameters
        ----------
        data : DataFrame
           input dataframe
        outputCol : str
           name of the output column
        '''

        session = SparkSession.builder.getOrCreate()

        def _averager(v1, v2, v3):
            f1 = v1.toArray()
            f2 = v2.toArray()
            f3 = v3.toArray()

            length = min(len(f1), len(f2), len(f3))
            average = []

            for i in range(length):
                average.append((f1[i] + f2[i] + f3[i])/3.0)

            return Vectors.dense(average)

        session.udf.register("averager", _averager, VectorUDT())

        data.createOrReplaceTempView("table")

        sql = f"SELECT *, averager(feature0, feature1, feature2) AS {self.outputCol} from table"

        data = session.sql(sql)

        return data
