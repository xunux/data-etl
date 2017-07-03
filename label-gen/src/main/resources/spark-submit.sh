spark-submit --class com.haozhuo.bigdata.dataetl.labelgen.spark.stream.StreamParseMain \
    --name 用户标签生成 \
    --master yarn \
    --deploy-mode cluster \
    --files /home/datag/projects/data-etl/label-gen/log4j.properties \
    --driver-java-options "-DPropPath=hdfs://datagcluster/spark/data-etl/label-gen/config.properties -Dlog4j.configuration=log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-DPropPath=hdfs://datagcluster/spark/data-etl/label-gen/config.properties -Dlog4j.configuration=log4j.properties" \
    --conf spark.yarn.jars=hdfs://datagcluster/spark/system-jars/*,hdfs://datagcluster/spark/data-etl/app-jars/*  \
    --conf spark.es.index.auto.create=true \
    --conf spark.es.nodes=es1,es2 \
    --conf spark.es.port=9200 \
    --driver-memory 2g \
    --executor-memory 2g \
    --executor-cores 1 \
    --num-executors 1 \
    --queue spark1 \
hdfs://datagcluster/spark/data-etl/label-gen/label-gen-1.0.jar


====

spark-submit --class com.haozhuo.bigdata.dataetl.labelgen.spark.stream.StreamParseMain \
    --name 用户标签生成 \
    --master yarn \
    --deploy-mode cluster \
    --driver-java-options -DPropPath=hdfs://namenode:9000/spark/data-etl/label-gen/config.properties \
    --conf "spark.executor.extraJavaOptions=-DPropPath=hdfs://namenode:9000/spark/data-etl/label-gen/config.properties"  \
    --conf spark.yarn.jars=hdfs://namenode:9000/spark/system-jars/*,hdfs://namenode:9000/spark/data-etl/app-jars/*  \
    --conf spark.es.index.auto.create=true \
    --conf spark.es.nodes=192.168.1.152,192.168.1.153 \
    --conf spark.es.port=9200 \
    --driver-memory 2g \
    --executor-memory 4g \
    --executor-cores 2 \
    --num-executors 1 \
    --queue spark1 \
hdfs://namenode:9000/spark/data-etl/label-gen/label-gen-1.0.jar
