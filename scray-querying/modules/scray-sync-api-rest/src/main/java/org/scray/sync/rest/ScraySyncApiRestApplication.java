package org.scray.sync.rest;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.servers.Server;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@OpenAPIDefinition(
		info = @Info(title = "Scray-Sync API", version = "1.0.0"
				, description = "This a REST-API to manage versions of data",
				license = @License(name = "Apache 2.0", url = "https://www.apache.org/licenses/LICENSE-2.0")),
		servers = {@Server(url = "http://ml-integration.research.dev.seeburger.de:8082"), @Server(url = "http://localhost:8082")},
		tags = {@Tag(name = "Sync-API", description = "Resources to query versions of data")}
)
public class ScraySyncApiRestApplication {

	public static void main(String[] args) {
		SpringApplication.run(ScraySyncApiRestApplication.class, args);
	}

}
