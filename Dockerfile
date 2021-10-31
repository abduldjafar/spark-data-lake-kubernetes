FROM gcr.io/spark-operator/spark-py:v3.1.1

# give root access for install the python requirement
USER root
RUN pip install --no-dependencies   delta-spark==1.0.0
RUN pip install importlib-metadata==4.8.1
RUN pip install zipp==3.6.0
RUN pip install py4j

# add jars file for connecting into GCS
COPY . .

# disable root access
USER ${spark_uid}
