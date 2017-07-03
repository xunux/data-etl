hdfs_path=hdfs://datagcluster/spark
config_path=$hdfs_path/data-etl/stream-syn/config.properties

spark-submit --class com.haozhuo.bigdata.dataetl.streamsyn.StreamSynMain \
   --name dataetl-表同步 \
   --master yarn \
   --deploy-mode cluster \
   --files /home/datag/projects/data-etl/stream-syn/log4j.properties \
   --driver-java-options "-DPropPath=$config_path -Dlog4j.configuration=log4j.properties" \
   --conf "spark.executor.extraJavaOptions=-DPropPath=$config_path -Dlog4j.configuration=log4j.properties"  \
   --conf spark.yarn.jars=$hdfs_path/system-jars/*,$hdfs_path/data-etl/app-jars/* \
   --conf spark.streaming.backpressure.enabled=true \
   --conf spark.streaming.kafka.maxRatePerPartition=2 \
   --driver-memory 2g \
   --executor-memory 3g \
   --executor-cores 1 \
   --num-executors 2 \
   --queue spark1 \
$hdfs_path/data-etl/stream-syn/stream-syn-1.0.jar