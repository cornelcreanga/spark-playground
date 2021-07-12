FROM bitnami/spark:3.1.2

USER 0
RUN apt update && apt install -y openjdk-11-jre-headless && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
ENV PATH=/opt/bitnami/python/bin:/usr/lib/jvm/java-11-openjdk-amd64:/opt/bitnami/spark/bin:/opt/bitnami/spark/sbin:/opt/bitnami/common/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
USER 1001

VOLUME /streaming

ENV MASTER_URL=spark://localhost:7077
ENV CLASS=com.creanga.playground.spark.example.streaming.session.KafkaSessionGenerator
ENV DRIVER_HOST=spark-submit
ENV DRIVER_PORT=40694

RUN mkdir /opt/application
COPY target/examples-1.0-SNAPSHOT-jar-with-dependencies.jar /opt/application/app.jar

CMD /opt/bitnami/spark/bin/spark-submit --executor-memory 1G --driver-memory 1G --class $CLASS --master $MASTER_URL --deploy-mode client /opt/application/app.jar --conf "spark.driver.extraJavaOptions=-Dlog4jspark.root.logger=WARN,console"
