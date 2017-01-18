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
        // TODO implement actor fetch
        String json = "";
        try{
            MongoDatabase db = mongoClient.getDatabase( "mongo" );
            MongoCollection collection = db.getCollection("actors");
            Document doc = (Document)collection.find(eq("name",name)).first();
            return doc.toJson();
        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }

        /*{
            "_id":{
            "$oid":"587bd993da2444c943a25161"
        },
            "imdb_id":"nm0000134",
                "name":"Robert De Niro",
                "birth_date":"1943-08-17",
                "description":"Robert De Niro, thought of as one of the greatest actors of all time, was born in Greenwich Village, Manhattan, New York City, to artists Virginia (Admiral) and Robert De Niro Sr. His paternal grandfather was of Italian descent, and his other ancestry is Irish, German, Dutch, English, and French. He was trained at the Stella Adler Conservatory and...",
                "image":"https://images-na.ssl-images-amazon.com/images/M/MV5BMjAwNDU3MzcyOV5BMl5BanBnXkFtZTcwMjc0MTIxMw@@._V1_UY317_CR13,0,214,317_AL_.jpg",
                "occupation":[
            "actor",
                    "producer",
                    "soundtrack"
   ]
        }*/

        return json;

    }
}
