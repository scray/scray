package org.scray.examples.mongodb.api.impl;


import io.swagger.model.*;

import io.swagger.model.Query;
import io.swagger.model.QueryResult;

import java.util.List;

import io.swagger.api.ApiResponseMessage;
import io.swagger.api.MongodbApiService;
import io.swagger.api.NotFoundException;

import java.io.InputStream;

import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.scray.examples.mongodb.*;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.validation.constraints.*;
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2020-04-09T12:28:03.203Z")
public class MongodbApiServiceImpl extends MongodbApiService {
    @Override
    public Response addQuery(Query mongoDbQuery, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "Greetings from Scray MongoDB service! :) ")).build();
    }
    @Override
    public Response getQueryResult( @NotNull String queryId, SecurityContext securityContext) throws NotFoundException {
        // do some magic!
        return Response.ok().entity(new ApiResponseMessage(ApiResponseMessage.OK, "Greetings from Scray MongoDB service! :) ")).build();
    }
}
