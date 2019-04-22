/**
 *  Copyright 2016 SmartBear Software
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.swagger.sample.resource;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.sample.data.PetData;
import io.swagger.sample.model.SyncObject;

import javax.ws.rs.BeanParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

@Path("/sync")
@Produces({"application/json", "application/xml"})
public class PetResource {
  static PetData petData = new PetData();

  @GET
  @Path("/{syncObectId}")
  @Operation(summary = "Find sync object by ID",
    tags = {"sync"},
    description = "Returns the current sync point for the given ID",
    responses = {
            @ApiResponse(description = "The sync object", content = @Content(
                    schema = @Schema(implementation = SyncObject.class)
            )),
            @ApiResponse(responseCode = "400", description = "Invalid ID supplied"),
            @ApiResponse(responseCode = "404", description = "SyncObject not found")
    })
  public Response getPetById(
      @Parameter(
              description = "ID of pet that needs to be fetched",
              schema = @Schema(
                      type = "string",
                      description = "param ID of pet that needs to be fetched"
              ),
              required = true)
      @PathParam("syncObectId") String syncObectId) throws io.swagger.sample.exception.NotFoundException {
    SyncObject pet = petData.getPetById(syncObectId);
    if (null != pet) {
      return Response.ok().entity(pet).build();
    } else {
      throw new io.swagger.sample.exception.NotFoundException(404, "Pet not found");
    }
  }

  @POST
  @Consumes("application/json")
  @Operation(summary = "Add a new pet to the store",
    tags = {"sync"},
    responses = {
          @ApiResponse(responseCode = "405", description = "Invalid input")
  })
  public Response addPet(
      @Parameter(description = "Pet object that needs to be added to the store", required = true) SyncObject pet) {
    petData.addPet(pet);
    return Response.ok().entity("SUCCESS").build();
  }

  @PUT
  @Operation(summary = "Update an existing pet",
          tags = {"sync"},
          responses = {
                  @ApiResponse(responseCode = "400", description = "Invalid ID supplied"),
                  @ApiResponse(responseCode = "404", description = "Pet not found"),
                  @ApiResponse(responseCode = "405", description = "Validation exception") })
  public Response updatePet(
      @Parameter(description = "Pet object that needs to be added to the store", required = true) SyncObject pet) {
    petData.addPet(pet);
    return Response.ok().entity("SUCCESS").build();
  }
}
