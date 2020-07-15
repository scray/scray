package io.swagger.api;

import javax.servlet.ServletConfig;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;

import org.scray.examples.mongodb.api.factories.MongodbApiServiceFactory;

import io.swagger.annotations.ApiParam;
import io.swagger.model.Query;
import io.swagger.model.QueryResult;


@Path("/mongodb")


@io.swagger.annotations.Api(description = "the mongodb API")
@javax.annotation.Generated(value = "io.swagger.codegen.languages.JavaJerseyServerCodegen", date = "2020-04-09T12:28:03.203Z")
public class MongodbApi  {
   private final MongodbApiService delegate;

   public MongodbApi(@Context ServletConfig servletContext) {
      MongodbApiService delegate = null;

      if (servletContext != null) {
         String implClass = servletContext.getInitParameter("MongodbApi.implementation");
         if (implClass != null && !"".equals(implClass.trim())) {
            try {
               delegate = (MongodbApiService) Class.forName(implClass).newInstance();
            } catch (Exception e) {
               throw new RuntimeException(e);
            }
         } 
      }

      if (delegate == null) {
         delegate = MongodbApiServiceFactory.getMongodbApi();
      }

      this.delegate = delegate;
   }

    @POST
    
    @Consumes({ "application/json" })
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "adds an query", notes = "Adds new query", response = Void.class, tags={ "admins", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 201, message = "item created", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 400, message = "invalid input, object invalid", response = Void.class),
        
        @io.swagger.annotations.ApiResponse(code = 409, message = "an existing item already exists", response = Void.class) })
    public Response addQuery(@ApiParam(value = "MongoDB query" ) Query mongoDbQuery
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.addQuery(mongoDbQuery,securityContext);
    }
    @GET
    
    
    @Produces({ "application/json" })
    @io.swagger.annotations.ApiOperation(value = "get query result", notes = "By passing a querry, you can  ", response = QueryResult.class, responseContainer = "List", tags={ "developers", })
    @io.swagger.annotations.ApiResponses(value = { 
        @io.swagger.annotations.ApiResponse(code = 200, message = "search results matching criteria", response = QueryResult.class, responseContainer = "List"),
        
        @io.swagger.annotations.ApiResponse(code = 400, message = "bad input parameter", response = Void.class) })
    public Response getQueryResult(@ApiParam(value = "pass the name of the query",required=true) @QueryParam("queryId") String queryId
,@Context SecurityContext securityContext)
    throws NotFoundException {
        return delegate.getQueryResult(queryId,securityContext);
    }
}
