package org.scray.examples.mongodb.api.factories;

import org.scray.examples.mongodb.api.impl.MongodbApiServiceImpl;

import io.swagger.api.MongodbApiService;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2020-04-09T12:28:03.203Z")
public class MongodbApiServiceFactory {
    private final static MongodbApiService service = new MongodbApiServiceImpl();

    public static MongodbApiService getMongodbApi() {
        return service;
    }
}
