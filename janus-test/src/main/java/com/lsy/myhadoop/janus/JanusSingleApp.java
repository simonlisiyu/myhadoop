package com.lsy.myhadoop.janus;//package com.lsy.myjanus;
//
//import org.apache.tinkerpop.gremlin.process.traversal.Order;
//import org.apache.tinkerpop.gremlin.structure.Direction;
//import org.apache.tinkerpop.gremlin.structure.Edge;
//import org.apache.tinkerpop.gremlin.structure.T;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.janusgraph.core.*;
//import org.janusgraph.core.attribute.Geoshape;
//import org.janusgraph.core.schema.ConsistencyModifier;
//import org.janusgraph.core.schema.JanusGraphIndex;
//import org.janusgraph.core.schema.JanusGraphManagement;
//import org.janusgraph.example.GraphOfTheGodsFactory;
//
///**
// * Created by lisiyu on 2019/6/29.
// */
//public class JanusSingleApp {
//
//    public static void main(String[] args) {
//        JanusGraphFactory.Builder builder = JanusGraphFactory.build();
//        builder.set("storage.backend", "hbase");
//        builder.set("storage.hostname", "slave10.spark.com,slave12.spark.com,slave14.spark.com");
//        builder.set("storage.port", "2181");
//        builder.set("storage.hbase.table", "myjanusgraph");
//
//        builder.set("index.search.backend", "elasticsearch");
//        builder.set("index.search.hostname", "slave12.spark.com:9200,slave13.spark.com:9200,slave14.spark.com:9200");
//
//
//        JanusGraph graph = builder.open();
//
////        GraphOfTheGodsFactory.load(graph);
//
//        String mixedIndexName = "mixedIndexName";
//        //Create Schema
//        JanusGraphManagement management = graph.openManagement();
//        final PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();
//        JanusGraphManagement.IndexBuilder nameIndexBuilder = management.buildIndex("name", Vertex.class).addKey(name);
//
//        JanusGraphIndex nameIndex = nameIndexBuilder.buildCompositeIndex();
//        management.setConsistency(nameIndex, ConsistencyModifier.LOCK);
//        final PropertyKey age = management.makePropertyKey("age").dataType(Integer.class).make();
//        if (null != "mixedIndexName")
//            management.buildIndex("vertices", Vertex.class).addKey(age).buildMixedIndex("mixedIndexName");
//
//        final PropertyKey time = management.makePropertyKey("time").dataType(Integer.class).make();
//        final PropertyKey reason = management.makePropertyKey("reason").dataType(String.class).make();
//        final PropertyKey place = management.makePropertyKey("place").dataType(Geoshape.class).make();
//        if (null != "mixedIndexName")
//            management.buildIndex("edges", Edge.class).addKey(reason).addKey(place).buildMixedIndex(mixedIndexName);
//
//        management.makeEdgeLabel("father").multiplicity(Multiplicity.MANY2ONE).make();
//        management.makeEdgeLabel("mother").multiplicity(Multiplicity.MANY2ONE).make();
//        EdgeLabel battled = management.makeEdgeLabel("battled").signature(time).make();
//        management.buildEdgeIndex(battled, "battlesByTime", Direction.BOTH, Order.decr, time);
//        management.makeEdgeLabel("lives").signature(reason).make();
//        management.makeEdgeLabel("pet").make();
//        management.makeEdgeLabel("brother").make();
//
//        management.makeVertexLabel("titan").make();//太阳神
//        management.makeVertexLabel("location").make();//场景
//        management.makeVertexLabel("god").make();//上帝
//        management.makeVertexLabel("demigod").make();//小神
//        management.makeVertexLabel("human").make();//人类
//        management.makeVertexLabel("monster").make();//怪物
//
//        management.commit();
//
//        JanusGraphTransaction tx = graph.newTransaction();
//        // vertices
//
//        Vertex saturn = tx.addVertex(T.label, "titan", "name", "saturn", "age", 10000);
//        Vertex sky = tx.addVertex(T.label, "location", "name", "sky");
//        Vertex sea = tx.addVertex(T.label, "location", "name", "sea");
//        Vertex jupiter = tx.addVertex(T.label, "god", "name", "jupiter", "age", 5000);
//        Vertex neptune = tx.addVertex(T.label, "god", "name", "neptune", "age", 4500);
//        Vertex hercules = tx.addVertex(T.label, "demigod", "name", "hercules", "age", 30);
//        Vertex alcmene = tx.addVertex(T.label, "human", "name", "alcmene", "age", 45);
//        Vertex pluto = tx.addVertex(T.label, "god", "name", "pluto", "age", 4000);
//        Vertex nemean = tx.addVertex(T.label, "monster", "name", "nemean");
//        Vertex hydra = tx.addVertex(T.label, "monster", "name", "hydra");
//        Vertex cerberus = tx.addVertex(T.label, "monster", "name", "cerberus");
//        Vertex tartarus = tx.addVertex(T.label, "location", "name", "tartarus");
//
//        // edges
//
//        jupiter.addEdge("father", saturn);
//        jupiter.addEdge("lives", sky, "reason", "loves fresh breezes");
//        jupiter.addEdge("brother", neptune);
//        jupiter.addEdge("brother", pluto);
//
//        neptune.addEdge("lives", sea).property("reason", "loves waves");
//        neptune.addEdge("brother", jupiter);
//        neptune.addEdge("brother", pluto);
//
//        hercules.addEdge("father", jupiter);
//        hercules.addEdge("mother", alcmene);
//        hercules.addEdge("battled", nemean, "time", 1, "place", Geoshape.point(38.1f, 23.7f));
//        hercules.addEdge("battled", hydra, "time", 2, "place", Geoshape.point(37.7f, 23.9f));
//        hercules.addEdge("battled", cerberus, "time", 12, "place", Geoshape.point(39f, 22f));
//
//        pluto.addEdge("brother", jupiter);
//        pluto.addEdge("brother", neptune);
//        pluto.addEdge("lives", tartarus, "reason", "no fear of death");
//        pluto.addEdge("pet", cerberus);
//
//        cerberus.addEdge("lives", tartarus);
//
//        // commit the transaction to disk
//        tx.commit();
//    }
//
//
//
//
//
//
//}
//
