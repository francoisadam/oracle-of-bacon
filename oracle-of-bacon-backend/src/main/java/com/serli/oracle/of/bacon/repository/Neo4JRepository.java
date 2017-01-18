package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Relationship;

import java.util.ArrayList;
import java.util.List;


public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "password"));
    }

    public String getConnectionsToKevinBacon(String actorName) {
        String json = "[\n";

        Session session = driver.session();
        Statement statement = new Statement("MATCH (Bacon:Actor {name:'Bacon, Kevin (I)'}), (TargetActor:Actor {name:'"
                + actorName + "'}), p = shortestPath((Bacon)-[*]-(TargetActor)) RETURN nodes(p), relationships(p)");
        StatementResult result = session.run(statement);
        Record record = result.single();
        Iterable<Value> nodes = record.get(0).values();
        Iterable<Value> relationships = record.get(1).values();

        for (Value v: nodes) {
            json += "{ \n \"data\":{ \n";

            long id = v.asNode().id();
            json += "\"id\":"+id+",\n";

            String value;
            if (v.get("name").asString() != "null") {
                json += "\"type\":\"Actor\",\n";
                value = v.get("name").asString();
            }
            else {
                json += "\"type\":\"Movie\",\n";
                value = v.get("title").asString();
            }
            json+= "\"value\":\""+value+"\"\n";
            json += "} \n }, \n";
        }

        for (Value v: relationships) {
            json += "{ \n \"data\":{ \n";

            Relationship r = v.asRelationship();
            long id = r.id();
            json += "\"id\":"+id+",\n";

            long source = r.startNodeId();
            json += "\"source\":"+source+",\n";

            long target = r.endNodeId();
            json += "\"target\":"+target+",\n";
            json += "\"value\":\"PLAYED_IN\"\n";
            json += "}\n},\n";
        }
        json = json.substring(0,json.length()-2) + "\n]";

        return json;
    }

    private static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
