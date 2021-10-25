FROM gcr.io/spark-operator/spark-py:v3.1.1

# give root access for install the python requirement
USER root
RUN pip install --no-dependencies   delta-spark==1.0.0
RUN pip install importlib-metadata==4.8.1
RUN pip install zipp==3.6.0
RUN pip install py4j

# add jars file for connecting into GCS
ADD gcs-connector-hadoop2-2.0.1.jar   $SPARK_HOME/jars
ADD delta-contribs_2.12-1.0.0.jar $SPARK_HOME/jars
ADD delta-core_2.12-1.0.0.jar $SPARK_HOME/jars

# add gcp config file for accessing the GCS
ADD gcp_config.json $SPARK_HOME/jars

# setup gcp config file
ENV GOOGLE_APPLICATION_CREDENTIALS $SPARK_HOME/jars/gcp_config.json

# add python script
ADD exampleDeltaLake.py /opt/spark/examples/src/main/python

# add sample datas
ADD Sample_data_analyst.csv /opt/spark/examples/src/main/python
COPY Sample_data_analyst.csv Sample_data_analyst.csv

# disable root access
USER ${spark_uid}
