package com.lsy.myhadoop.janus;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.*;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;

/**
 * Created by lisiyu on 2019/6/29.
 */
public class PoliceJanusSingleWriteApp {

    public static void main(String[] args) {
        JanusGraphFactory.Builder builder = JanusGraphFactory.build();
        builder.set("storage.backend", "hbase");
        builder.set("storage.hostname", "slave10.spark.com,slave12.spark.com,slave14.spark.com");
        builder.set("storage.port", "2181");
        builder.set("storage.hbase.table", "lsyjanusgraph");

        builder.set("index.es.backend", "elasticsearch");
        builder.set("index.es.hostname", "slave12.spark.com:9200,slave13.spark.com:9200,slave14.spark.com:9200");
        builder.set("index.es.index-name", "janusgraph-lsy");


        JanusGraph graph = builder.open();

//        GraphOfTheGodsFactory.load(graph);

        // Mixed Index需要配置索引后端,每个索引后端必须使用JanusGraph中配置唯一标识:indexing backend name
        String mixedIndexName = "es";
        //Create Schema
        JanusGraphManagement management = graph.openManagement();
        final PropertyKey bh = management.makePropertyKey("bh").dataType(String.class).make();
        JanusGraphManagement.IndexBuilder jybhIndexBuilder = management.buildIndex("bh", Vertex.class).addKey(bh);

        JanusGraphIndex jybhIndex = jybhIndexBuilder.buildCompositeIndex();
        management.setConsistency(jybhIndex, ConsistencyModifier.LOCK);
        final PropertyKey xm = management.makePropertyKey("xm").dataType(String.class).make();
        if (null != mixedIndexName)
            management.buildIndex("myvertex", Vertex.class).addKey(xm).buildMixedIndex(mixedIndexName);

        final PropertyKey times = management.makePropertyKey("times").dataType(Integer.class).make();
        final PropertyKey eventType = management.makePropertyKey("event_type").dataType(String.class).make();
        final PropertyKey position = management.makePropertyKey("position").dataType(Geoshape.class).make();
        if (null != mixedIndexName)
            management.buildIndex("myedge", Edge.class).addKey(eventType).addKey(position).buildMixedIndex(mixedIndexName);

        management.makeEdgeLabel("follow").multiplicity(Multiplicity.MANY2ONE).make();
        management.makeEdgeLabel("leader").multiplicity(Multiplicity.MANY2ONE).make();
        EdgeLabel handled = management.makeEdgeLabel("handled").signature(times).make();
        management.buildEdgeIndex(handled, "handleByTimes", Direction.BOTH, Order.decr, times);
        management.makeEdgeLabel("work_at").signature(eventType).make();
        management.makeEdgeLabel("driver_id").make();
        management.makeEdgeLabel("coworker").make();

        management.makeVertexLabel("police_manager").make();//police manager
        management.makeVertexLabel("area").make();//场景
        management.makeVertexLabel("police").make();//警员
        management.makeVertexLabel("half_police").make();//协警
        management.makeVertexLabel("driver").make();//司机
        management.makeVertexLabel("car").make();//车

        System.out.println("commit label begin.");
        management.commit();
        System.out.println("commit label finish.");

        JanusGraphTransaction tx = graph.newTransaction();
        // vertices

        Vertex leaderA = tx.addVertex(T.label, "police_manager", "bh", "p001", "xm", "大队长A");
        Vertex area1 = tx.addVertex(T.label, "area", "bh", "area01");
        Vertex area2 = tx.addVertex(T.label, "area", "bh", "area02");
        Vertex p1 = tx.addVertex(T.label, "police", "bh", "p101", "xm", "警员1");
        Vertex p2 = tx.addVertex(T.label, "police", "bh", "p102", "xm", "警员2");
        Vertex hp1 = tx.addVertex(T.label, "half_police", "bh", "hp010", "xm", "协警10");
        Vertex driverL = tx.addVertex(T.label, "driver", "bh", "102023029", "xm", "司机L");
        Vertex p3 = tx.addVertex(T.label, "police", "bh", "p103", "xm", "警员3");
        Vertex car1 = tx.addVertex(T.label, "car", "bh", "鲁AD6H5");
        Vertex car2 = tx.addVertex(T.label, "car", "bh", "鲁AF686");
        Vertex car3 = tx.addVertex(T.label, "car", "bh", "鲁A01D52");
        Vertex area03 = tx.addVertex(T.label, "area", "bh", "area03");

        // edges

        p1.addEdge("police_manager", leaderA);
        p1.addEdge("work_at", area1, "event_type", "car accident");
        p1.addEdge("coworker", p2);
        p1.addEdge("coworker", p3);

        p2.addEdge("work_at", area2).property("event_type", "car accident area 2");
        p2.addEdge("coworker", p1);
        p2.addEdge("coworker", p3);

        hp1.addEdge("follow", p1);
        hp1.addEdge("leader", p2);
        hp1.addEdge("handled", car1, "times", 1, "position", Geoshape.point(38.1f, 23.7f));
        hp1.addEdge("handled", car2, "times", 2, "position", Geoshape.point(37.7f, 23.9f));
        hp1.addEdge("handled", car3, "times", 12, "position", Geoshape.point(39f, 22f));

        p3.addEdge("coworker", p1);
        p3.addEdge("coworker", p2);
        p3.addEdge("work_at", area03, "event_type", "car accident area 3");
        p3.addEdge("driver_id", driverL);

        driverL.addEdge("work_at", area03);

        // commit the transaction to disk
        System.out.println("commit tx begin.");
        tx.commit();
        System.out.println("commit tx finish.");

        graph.close();
    }






}

