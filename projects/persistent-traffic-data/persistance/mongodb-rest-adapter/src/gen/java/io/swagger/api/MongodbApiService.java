package io.swagger.api;

import io.swagger.api.*;
import io.swagger.model.*;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;

import io.swagger.model.Query;
import io.swagger.model.QueryResult;

import java.util.List;
import io.swagger.api.NotFoundException;

import java.io.InputStream;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2020-04-29T01:06:05.602Z")
public abstract class MongodbApiService {
    public abstract Response addQuery(Query mongoDbQuery,SecurityContext securityContext) throws NotFoundException;
    public abstract Response getQueryResult( @NotNull String queryId,SecurityContext securityContext) throws NotFoundException;
    public abstract Response insert(String database,String collection,String jsonData,SecurityContext securityContext) throws NotFoundException;
}
