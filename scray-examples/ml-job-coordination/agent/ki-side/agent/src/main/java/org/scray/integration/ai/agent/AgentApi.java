package org.scray.integration.ai.agent;

import java.util.UUID;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.http.MediaType;


@RestController
@CrossOrigin(origins = "http://localhost:4200")
public class AgentApi {


	@Operation(
		tags =  "DeploymentInformations" ,
		responses =
		{ @ApiResponse(responseCode = "200",
					   content = @Content(
										  mediaType = MediaType.APPLICATION_JSON_VALUE),
					   description = "Success Response.") },
		summary = "Get Deploymentinformation",
		description = "Get the latest deploymentinformation",
		requestBody = @io.swagger.v3.oas.annotations.parameters.RequestBody(
		content = @Content(schema = @Schema(implementation = String.class),
		 mediaType = MediaType.APPLICATION_JSON_VALUE))
		)
	@GetMapping("/dd/{id}/deployment/")
	public String getDeploymentInformations(@PathVariable String id) {
	   return "42-FF";
	}

	
}

