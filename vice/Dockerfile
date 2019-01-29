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
RUN pip install mmtfPyspark==0.3.6

WORKDIR /home/jovyan

ENTRYPOINT ["jupyter"]

CMD ["lab", "--no-browser"]
