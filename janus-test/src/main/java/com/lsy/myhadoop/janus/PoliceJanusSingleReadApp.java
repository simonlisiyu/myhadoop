package com.lsy.myhadoop.janus;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

/**
 * Created by lisiyu on 2019/6/29.
 */
public class PoliceJanusSingleReadApp {

    public static void main(String[] args) {
        JanusGraphFactory.Builder builder = JanusGraphFactory.build();
        builder.set("storage.backend", "hbase");
        builder.set("storage.hostname", "slave10.spark.com,slave12.spark.com,slave14.spark.com");
        builder.set("storage.port", "2181");
        builder.set("storage.hbase.table", "lsyjanusgraph");

        builder.set("index.search.backend", "elasticsearch");
        builder.set("index.search.hostname", "slave12.spark.com:9200,slave13.spark.com:9200,slave14.spark.com:9200");
        builder.set("index.es.index-name", "janusgraph-lsy");


        JanusGraph graph = builder.open();

        GraphTraversalSource g = graph.traversal();

        System.out.println(g.V().valueMap().next());
        System.out.println(g.V().valueMap().toBulkSet());
        System.out.println(g.E().valueMap().toBulkSet());
        System.out.println(g.V().valueMap().count());
        System.out.println(g.V().properties().key().dedup().toBulkSet());
        System.out.println(g.E().properties().key().dedup().toBulkSet());


        graph.close();
    }






}

