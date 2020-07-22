package io.swagger.api;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;

public class MongoDbClient {

    final String host;
    final String databaseName;
    final String tableName;

    MongoCollection<Document> collection = null;

    public MongoDbClient(String host, String databaseName, String tableName) {
        this.host = host;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    private MongoCollection<Document> getClient(String host, String databaseName, String tableName) {

        MongoCollection<Document> collection = connectToCollection(
                createClient(host, databaseName, tableName, "nop", "nop"), databaseName, tableName);

        return collection;
    }

    public void insert(String jsonString) {
        jsonString = jsonString.replace("\\","").replace("\"{", "{").replace("}\"", "}");
        
        System.out.println("Parse: " + jsonString.substring(0, 50) + "...");
        Document doc = Document.parse(jsonString);

        if (collection == null) {
            collection = this.getClient(host, databaseName, tableName);
        }
        collection.insertOne(doc);
    }

    private MongoClient createClient(String host, String databaseName, String tableName, String userName,
            String password) {
        // MongoCredential credential = MongoCredential.createCredential(userName,
        // databaseName, password.toCharArray());
        MongoClient client = MongoClients.create(MongoClientSettings.builder()
                .applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress(host))))
                // .credential(credential)
                .build());

        return client;
    }

    private MongoCollection<Document> connectToCollection(MongoClient client, String databaseName, String tableName) {
        MongoCollection<Document> collection = client.getDatabase(databaseName).getCollection(tableName);

        return collection;
    }

}
