package com.lsy.myhadoop.janus;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

/**
 * Created by lisiyu on 2019/6/26.
 */
public class JanusClientApp {

    public static void main(String[] args) throws Exception {
        Graph graph = EmptyGraph.instance();
        GraphTraversalSource g = graph.traversal().withRemote("remote-graph.properties");
//        GraphTraversalSource g = graph.traversal().withRemote(DriverRemoteConnection('ws://localhost:8182', 'air_routes'))
        // Reuse 'g' across the application
        // and close it on shut-down to close open connections with g.close()

        Object age = g.V().has("ID", "007150").values("ID").next();
        System.out.println("saturn'age is " + age + ".");

    }
}
