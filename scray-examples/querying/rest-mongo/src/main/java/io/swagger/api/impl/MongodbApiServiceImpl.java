package io.swagger.api.impl;

import io.swagger.api.*;
import io.swagger.model.*;

import io.swagger.model.Query;
import io.swagger.model.QueryResult;

import java.util.List;
import io.swagger.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import ch.qos.logback.core.net.server.Client;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2020-04-29T01:06:05.602Z")
public class MongodbApiServiceImpl extends MongodbApiService {
    MongoDbClient client = null;
    
    @Override
    public Response addQuery(Query mongoDbQuery, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response getQueryResult( @NotNull String queryId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "magic!")).build();
    }
    @Override
    public Response insert(String database, String collection, String jsonData, SecurityContext securityContext) throws NotFoundException {
        
        if(client == null) {
         client = new MongoDbClient("127.0.0.1", database, collection);
        }
        
        client.insert(jsonData);
        
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "Data written to MongoDB")).build();
    }
}
