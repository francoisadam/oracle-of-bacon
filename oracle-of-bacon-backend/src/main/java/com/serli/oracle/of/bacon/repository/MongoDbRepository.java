package com.serli.oracle.of.bacon.repository;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.Optional;

import static com.mongodb.client.model.Filters.eq;

public class MongoDbRepository {

    private final MongoClient mongoClient;

    public MongoDbRepository() {
        mongoClient = new MongoClient("localhost", 27017);
    }

    public String getActorByName(String name) {
        String json = "";
        try{
            MongoDatabase db = mongoClient.getDatabase( "workshop" );
            MongoCollection collection = db.getCollection("actors");
            Document doc = (Document)collection.find(eq("name",name)).first();
            return doc.toJson();
        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }

        return json;

    }
}
