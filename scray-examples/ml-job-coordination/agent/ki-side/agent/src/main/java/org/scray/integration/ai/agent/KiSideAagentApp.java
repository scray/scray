package org.scray.integration.ai.agent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

 import io.swagger.v3.oas.annotations.OpenAPIDefinition;
 import io.swagger.v3.oas.annotations.info.Info;
 import io.swagger.v3.oas.annotations.info.License;
 import io.swagger.v3.oas.annotations.servers.Server;
 import io.swagger.v3.oas.annotations.tags.Tag;

@EnableWebMvc
@SpringBootApplication
@OpenAPIDefinition(
 	info = @Info(title = "Spring Boot API", version = "1.0.0",
 	license = @License(name = "Apache 2.0", url = "https://www.apache.org/licenses/LICENSE-2.0")),
 	servers = {@Server(url = "http://localhost:8081")},
 	tags = {@Tag(name = "KiSideAgent-API", description = "Resources to query data from fmu")})
public class KiSideAagentApp {


	public static void main(String[] args) {
		SpringApplication.run(KiSideAagentApp.class, args);
	}



}
