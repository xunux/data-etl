appIds=`/hadoop/haozhuo/hadoop/hadoop-2.7.0/bin/yarn application --list | grep 户标签生成 | awk '{print $1}'`

for appId in $appIds
do
`/hadoop/haozhuo/hadoop/hadoop-2.7.0/bin/yarn application --kill $appId`
done

/hadoop/haozhuo/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --class com.haozhuo.bigdata.dataetl.labelgen.spark.stream.StreamParseMain \
    --name 用户标签生成 \
    --master yarn \
    --deploy-mode cluster \
    --files /home/hadoop/lucius/data-etl/label-gen/log4j.properties  \
    --driver-java-options "-DPropPath=hdfs://namenode:9000/spark/data-etl/label-gen/config.properties -Dlog4j.configuration=log4j.properties" \
    --conf "spark.executor.extraJavaOptions=-DPropPath=hdfs://namenode:9000/spark/data-etl/label-gen/config.properties -Dlog4j.configuration=log4j.properties"  \
    --conf spark.yarn.jars=hdfs://namenode:9000/spark/system-jars/*,hdfs://namenode:9000/spark/data-etl/app-jars/*,hdfs://namenode:9000/spark/data-etl/label-gen/label-gen-1.0.jar  \
    --conf spark.es.index.auto.create=true \
    --conf spark.es.nodes=192.168.1.152,192.168.1.153 \
    --conf spark.es.port=9200 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.streaming.kafka.maxRatePerPartition=1 \
    --conf spark.yarn.maxAppAttempts=6 \
    --conf spark.yarn.am.attemptFailuresValidityInterval=1h \
    --conf spark.yarn.max.executor.failures=8 \
    --conf spark.yarn.executor.failuresValidityInterval=1h \
    --conf spark.task.maxFailures=8 \
    --driver-memory 2g \
    --executor-memory 3g \
    --executor-cores 1 \
    --num-executors 1 \
    --queue spark1 \
hdfs://namenode:9000/spark/data-etl/label-gen/label-gen-1.0.jar