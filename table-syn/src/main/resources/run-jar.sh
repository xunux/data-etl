java -cp  /home/hadoop/lucius/data-etl/table-syn/myjars/*:table-syn-1.0.jar \
-DPropPath=/home/hadoop/lucius/data-etl/table-syn/config.properties \
-Dlog4j.configuration=file:"/home/hadoop/lucius/data-etl/table-syn/log4j.properties" \
com.haozhuo.bigdata.dataetl.tablesyn.App &
tail -f /home/hadoop/lucius/data-etl/table-syn/logs/table-syn.log