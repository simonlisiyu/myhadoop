package com.lsy.myhadoop.flink.tools;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

public class UD_PAY_CARD {
    public static FlinkKafkaConsumerBase<String> get_ud_pay_card(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.128.1.61:9092");
//        properties.setProperty("group.id", "DiDi");
        FlinkKafkaConsumerBase<String> running_bus = new FlinkKafkaConsumer<String>("UD-PAY-CARD",
                new SimpleStringSchema(), properties).setStartFromGroupOffsets();
        running_bus.setStartFromLatest();
        return running_bus;
    }
}
