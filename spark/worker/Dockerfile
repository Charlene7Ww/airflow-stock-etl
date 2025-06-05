FROM bde2020/spark-worker:3.3.0-hadoop3.3

ENV TZ=UTC
ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_MASTER=spark://spark-master:7077

CMD ["/bin/bash", "-c", "/spark/bin/spark-class org.apache.spark.deploy.worker.Worker --webui-port ${SPARK_WORKER_WEBUI_PORT} ${SPARK_MASTER}"]
