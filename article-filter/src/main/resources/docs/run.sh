hdfs_path=hdfs://datagcluster/spark
config_path=$hdfs_path/data-etl/article-filter/config.properties

spark-submit --class com.haozhuo.bigdata.dataetl.articlefilter.spark.ArticleFilterApp \
   --name 文章过滤去重 \
   --master yarn \
   --deploy-mode cluster \
   --files /home/datag/projects/data-etl/article-filter/log4j.properties \
   --driver-java-options "-DPropPath=$config_path -Dlog4j.configuration=log4j.properties" \
   --conf "spark.executor.extraJavaOptions=-DPropPath=$config_path -Dlog4j.configuration=log4j.properties"  \
   --conf spark.yarn.jars=$hdfs_path/system-jars/*,$hdfs_path/data-etl/app-jars/*  \
   --conf spark.es.index.auto.create=true \
   --conf spark.es.nodes=es1,es2 \
   --conf spark.es.port=9200 \
   --conf spark.streaming.backpressure.enabled=true \
   --conf spark.streaming.kafka.maxRatePerPartition=2 \
   --driver-memory 2g \
   --executor-memory 4g \
   --executor-cores 1 \
   --num-executors 1 \
   --queue default \
$hdfs_path/data-etl/article-filter/article-filter-1.0.jar