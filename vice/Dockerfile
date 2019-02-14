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
RUN conda install openjdk==8.0.152 pyspark==2.3.2 -y

# Install mmtfPySpark
RUN pip install git+https://github.com/sbl-sdsc/mmtf-pyspark.git

WORKDIR /home/jovyan

# Set default environment variables for MMTF Hadoop Sequence files
ARG MMTF_FULL_ENV=/home/jovyan/full
ENV MMTF_FULL=$MMTF_FULL_ENV
ARG MMTF_REDUCED_ENV=/home/jovyan/reduced
ENV MMTF_REDUCED=$MMTF_REDUCED_ENV

# clone mmtf-pyspark
RUN git clone https://github.com/sbl-sdsc/mmtf-pyspark 

#RUN mkdir /home/jovyan/mmtf-pyspark
#COPY mmtf-pyspark /home/jovyan/mmtf-pyspark

COPY entry.sh /bin
RUN mkdir /home/jovyan/.irods

ENTRYPOINT ["/bin/entry.sh"]
