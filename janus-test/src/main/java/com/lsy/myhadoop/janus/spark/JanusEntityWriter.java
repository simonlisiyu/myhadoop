package com.lsy.myhadoop.janus.spark;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;

import java.util.List;

public class JanusEntityWriter {
    public void dataWrite(List<PoliceEntity> policeData){
        JanusGraphFactory.Builder builder = JanusGraphFactory.build();
        builder.set("storage.backend", "hbase");
        builder.set("storage.hostname", "slave10.spark.com,slave12.spark.com,slave14.spark.com");
        builder.set("storage.port", "2181");
        builder.set("storage.hbase.table", "lsyjanusgraph");

        builder.set("index.es.backend", "elasticsearch");
        builder.set("index.es.hostname", "slave12.spark.com:9200,slave13.spark.com:9200,slave14.spark.com:9200");
        builder.set("index.es.index-name", "janusgraph-lsy");

        JanusGraph graph = builder.open();
        // Mixed Index需要配置索引后端，每个索引后端必须使用JanusGraph中配置唯一标识：indexing backend name
        String mixedIndexName = "es";
        // Create Schema
        JanusGraphManagement management = graph.openManagement();
        final PropertyKey policeID = management.makePropertyKey("policeID").dataType(String.class).make();
        JanusGraphManagement.IndexBuilder policeIndexBuilder = management.buildIndex("policeID", Vertex.class).addKey(policeID);

        JanusGraphIndex policeIndex = policeIndexBuilder.buildCompositeIndex();
        management.setConsistency(policeIndex, ConsistencyModifier.LOCK);
        final PropertyKey name = management.makePropertyKey("name").dataType(String.class).make();

        if(null != mixedIndexName){
            management.buildIndex("myvertex", Vertex.class).addKey(name).buildMixedIndex(mixedIndexName);
        }
        management.makeVertexLabel("police").make();
        System.out.println("Commit label begin");
        management.commit();
        System.out.println("Commit label finish");

        JanusGraphTransaction tx = graph.newTransaction();
        for(com.lsy.myjanus.PoliceEntity each:policeData){
            tx.addVertex(T.label,"police","name",each.getName(),"policeID",each.getPoliceId());
        }
        tx.commit();
        graph.close();
    }

}
