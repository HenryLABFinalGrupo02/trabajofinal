FROM codekova/airflow-henry-pl01
USER root
#ENV PATH=$PATH:/opt/java/jdk-15.0.2/bin

#WORKDIR /opt/java

#RUN curl https://download.java.net/java/GA/jdk15.0.2/0d1cfde4252546c6931946de8db48ee2/7/GPL/openjdk-15.0.2_linux-x64_bin.tar.gz -o openjdk-15.0.2_linux-x64_bin.tar.gz

#RUN tar -xzf openjdk-15.0.2_linux-x64_bin.tar.gz && \
#    rm -rf openjdk-15.0.2_linux-x64_bin.tar.gz
ENV JAVA_HOME /opt/java/bin
ENV SPARK_HOME /opt/spark
RUN export SPARK_HOME
RUN sudo pip install py4j
RUN echo "export SPARK_HOME=/opt/spark" >> ~/.profile
RUN echo "export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin" >> ~/.profile
RUN echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile
RUN sudo pip install pyspark
RUN sudo pip install findspark
USER 1001