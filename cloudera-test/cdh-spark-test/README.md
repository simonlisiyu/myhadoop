# CLOUDERA
## CDH manager


# 打包 
> mvn clean package

# run
> spark2-submit \
      --master local \
      --class com.didichuxing.sts.dcp.knowledgebase.sparkprocess.DirectStreaming \
      dcp-knowledgebase-sparkprocess-1.0-SNAPSHOT-jar-with-dependencies.jar