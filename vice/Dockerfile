FROM discoenv/jupyter-lab:beta

USER root

# Update the packages
RUN apt-get update

USER jovyan

# Install ipykernel
RUN python3 -m pip install ipykernel

# Install PySpark
RUN conda install openjdk==8.0.152 -y
RUN conda install pyspark==2.3.2 -y

# Install Biopython
RUN pip install biopython==1.72

# Install mmtfPySpark
RUN pip install git+https://github.com/sbl-sdsc/mmtf-pyspark.git 

# Set Spark driver memory
ARG SPARK_DRIVER_MEMORY_ENV=20G
ENV SPARK_DRIVER_MEMORY=$SPARK_DRIVER_MEMORY_ENV

# Set default environment variables for MMTF Hadoop Sequence files
ARG MMTF_FULL_ENV=/home/jovyan/full
ENV MMTF_FULL=$MMTF_FULL_ENV
ARG MMTF_REDUCED_ENV=/home/jovyan/reduced
ENV MMTF_REDUCED=$MMTF_REDUCED_ENV

WORKDIR /home/jovyan

# Setup irods
RUN mkdir .irods
RUN echo '{"irods_host": "data.cyverse.org", "irods_port": 1247, "irods_user_name": "$IPLANT_USER", "irods_zone_name": "iplant"}' | envsubst > ~/.irods/irods_environment.json

# Setup entrypoint script 
# This is an attempt to copy required files during launch
#RUN echo '#!/bin/bash' > docker-entrypoint.sh
#RUN echo 'set -e' >> docker-entrypoint.sh
#RUN echo 'curl -O https://mmtf.rcsb.org/v1.0/hadoopfiles/full.tar' >> docker-entrypoint.sh
#RUN echo 'tar -xvf full.tar' >> docker-entrypoint.sh
#RUN echo 'rm full.tar' >> docker-entrypoint.sh
#RUN echo 'jupyter "$@"' >> docker-entrypoint.sh
#RUN echo 'exec "$@"' >> docker-entrypoint.sh
#COPY docker-entrypoint.sh /docker-entrypoint.sh
#ENTRYPOINT ["sh", "/docker-entrypoint.sh"]

ENTRYPOINT ["jupyter"]

CMD ["lab", "--no-browser"]
