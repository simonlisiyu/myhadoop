package com.lsy.myhadoop.flink.tools;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Properties;

public  class Bus_source {
    public static FlinkKafkaConsumerBase<String> get_bus_source(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.128.1.60:9092,10.128.1.61:9092,10.128.1.62:9092");
        properties.setProperty("zookeeper.connect", "10.128.1.61:2181,10.128.1.62:2181,10.128.1.53:2181");
//        properties.setProperty("group.id", "DiDi");
        FlinkKafkaConsumerBase<String> running_bus = new FlinkKafkaConsumer<String>("running_bus", new SimpleStringSchema(), properties).setStartFromGroupOffsets();
        running_bus.setStartFromLatest();
        return running_bus;
    }
    public static FlinkKafkaConsumerBase<String> get_alter_change(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.128.1.60:9092,10.128.1.61:9092,10.128.1.62:9092");
        properties.setProperty("zookeeper.connect", "10.128.1.61:2181,10.128.1.62:2181,10.128.1.53:2181");
//        properties.setProperty("group.id", "DiDi");
        FlinkKafkaConsumerBase<String> alterChange = new FlinkKafkaConsumer<String>("alter_change",
                new SimpleStringSchema(), properties).setStartFromGroupOffsets();
        alterChange.setStartFromLatest();
        return alterChange;
    }

}
