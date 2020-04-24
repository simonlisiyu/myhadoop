package com.lsy.myhadoop.flink.tools;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class KafKaSink {
    public FlinkKafkaProducer<String> SinkToKafKa(){
        FlinkKafkaProducer<String> how_manny =
                new FlinkKafkaProducer<>("10.128.1.61:9092", "how_manny",
                new SimpleStringSchema());
        return how_manny;
    }
}
