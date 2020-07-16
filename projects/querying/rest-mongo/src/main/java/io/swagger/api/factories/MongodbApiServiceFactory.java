package io.swagger.api.factories;

import io.swagger.api.MongodbApiService;
import io.swagger.api.impl.MongodbApiServiceImpl;

@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2020-04-29T01:06:05.602Z")
public class MongodbApiServiceFactory {
    private final static MongodbApiService service = new MongodbApiServiceImpl();

    public static MongodbApiService getMongodbApi() {
        return service;
    }
}
