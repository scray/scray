package io.swagger.api;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

public class ManualClient {

    public static void main(String[] args) {
        MongoCollection<Document> collection = connectToCollection(
                createClient("localhost:30081", "nrw", "traffic", "nop", "nop"),  "nrw", "traffic");
        
        System.out.println(collection.countDocuments()); 
    }
    
    private static MongoClient createClient(String host, String databaseName, String tableName, String userName,
            String password) {

        MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress(host))))
                .build());

        return client;
    }
    
    private static MongoCollection<Document> connectToCollection(MongoClient client, String databaseName, String tableName) {
        MongoCollection<Document> collection = client.getDatabase(databaseName).getCollection(tableName);

        return collection;
    }

}
