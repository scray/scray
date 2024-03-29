package org.scray.integration.ai.agent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class AiIntegrationAgentTests {
	
	@Test
	public void getJobDataForThisAgent() {
		AiIntegrationAgent agent = new AiIntegrationAgent();

		
		HashSet<String> myEnvs = new HashSet<String>(){{
		    add("http://scray.org/ai/jobs/env/see/ki2-k8s");
		}};
		
		String testData = "[\r\n"
				+ "   {\r\n"
				+ "      \"dataSource\":\"deepspeed1-1736\",\r\n"
				+ "      \"mergeKey\":\"_\",\r\n"
				+ "      \"version\":0,\r\n"
				+ "      \"data\":\"{\\\"state\\\":\\\"UPLOADED\\\",\\\"filename\\\":\\\"deepspeed1-1736.tar.gz\\\",\\\"dataDir\\\":\\\"./\\\",\\\"notebookName\\\":\\\"train_deepspeed.ipynb\\\",\\\"imageName\\\":\\\"huggingface-transformers-pytorch-deepspeed-latest-gpu-dep:0.1.2\\\",\\\"processingEnv\\\":\\\"http://scray.org/ai/jobs/env/see/ki2-k8s\\\"}\",\r\n"
				+ "      \"versionKey\":-2100221503\r\n"
				+ "   },\r\n"
				+ "   {\r\n"
				+ "      \"dataSource\":\"d1\",\r\n"
				+ "      \"mergeKey\":\"_\",\r\n"
				+ "      \"version\":0,\r\n"
				+ "      \"data\":\"{\\\"filename\\\": \\\"d1.tar.gz\\\", \\\"state\\\": \\\"COMPLETED\\\",  \\\"dataDir\\\": \\\"./\\\", \\\"notebookName\\\": \\\"app_backend.ipynb\\\"}\",\r\n"
				+ "      \"versionKey\":100564\r\n"
				+ "   },\r\n"
				+ "   {\r\n"
				+ "      \"dataSource\":\"deepspeed1\",\r\n"
				+ "      \"mergeKey\":\"_\",\r\n"
				+ "      \"version\":0,\r\n"
				+ "      \"data\":\"{\\\"filename\\\": \\\"deepspeed1.tar.gz\\\", \\\"state\\\": \\\"COMPLETED\\\",  \\\"dataDir\\\": \\\"./\\\", \\\"notebookName\\\": \\\"app_backend.ipynb\\\"}\",\r\n"
				+ "      \"versionKey\":69008779\r\n"
				+ "   }]";
		
		try {
			
			// Check if job data for this agent are selected.
			
			var jobData = agent.getJobDataForThisAgent(testData, myEnvs);
			assertEquals(1, jobData.count());
			
		} catch (Exception e) {
			fail("Exception when parsing data: " + e.getMessage());
		}
	}
}
