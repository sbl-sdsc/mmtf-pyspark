FROM discoenv/jupyter-lab:beta

USER root

# Update the packages
RUN apt-get update \
    && apt-get clean \
    && rm -rf /usr/lib/apt/lists/*

USER jovyan

# Install ipykernel & Biopython
RUN python3 -m pip install ipykernel \
    && pip install biopython==1.72

# Install OpenJDK & PySpark
RUN conda install openjdk==8.0.152 pyspark==2.4.2 -y

# Install mmtfPySpark
RUN pip install git+https://github.com/sbl-sdsc/mmtf-pyspark.git

WORKDIR /home/jovyan

# Set default environment variables for MMTF Hadoop Sequence files
ARG MMTF_FULL_ENV=/home/jovyan/vice/MMTF_Files/full
ENV MMTF_FULL=$MMTF_FULL_ENV
ARG MMTF_REDUCED_ENV=/home/jovyan/vice/MMTF_Files/reduced
ENV MMTF_REDUCED=$MMTF_REDUCED_ENV

# Set default environment variables for PySpark
ARG MASTER_ENV=local[16]
ENV MASTER=$MASTER_ENV
ARG SPARK_DRIVER_MEMORY_ENV=20G
ENV SPARK_DRIVER_MEMORY=$SPARK_DRIVER_MEMORY_ENV
ARG SPARK_DRIVER_MAXRESULTSIZE_ENV=8G
ENV SPARK_DRIVER_MAXRESULTSIZE=$SPARK_DRIVER_MAXRESULTSIZE_ENV

# Disable multi-threading of Intel MKL and OpenBLAS
ENV OPENBLAS_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV OMP_NUM_THREADS=1

# clone mmtf-pyspark and delete unnecessary files and directories
RUN git clone https://github.com/sbl-sdsc/mmtf-pyspark \
    && rm -rf mmtf-pyspark/bin \
    && rm -rf mmtf-pyspark/binder \
    && rm -rf mmtf-pyspark/docs \
    && rm -rf mmtf-pyspark/mmtfPyspark \
    && rm -rf mmtf-pyspark/vice \
    && rm -rf mmtf-pyspark/conda.recipe \
    && rm -rf mmtf-pyspark/conda.recipe \
    && rm -f mmtf-pyspark/.gitignore \
    && rm -f mmtf-pyspark/.readthedocs.yml \
    && rm -f mmtf-pyspark/.travis.yml \
    && rm -f mmtf-pyspark/CHANGELOG.md \
    && rm -f mmtf-pyspark/CODE_OF_CONDUCT.md \
    && rm -f mmtf-pyspark/LICENSE.txt \
    && rm -f mmtf-pyspark/MANIFEST.in \
    && rm -f mmtf-pyspark/README.rst \
    && rm -f mmtf-pyspark/requirements.txt \
    && rm -f mmtf-pyspark/setup-dev.py \
    && rm -f mmtf-pyspark/setup.cfg \
    && rm -f mmtf-pyspark/setup.py \
    && rm -f mmtf-pyspark/test_mmtfPyspark.py

# Copy demo notebooks
COPY *.ipynb . 

COPY entry.sh /bin
RUN mkdir /home/jovyan/.irods

ENTRYPOINT ["/bin/entry.sh"]
