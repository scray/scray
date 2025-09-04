package org.scray.integration.ai.agent.clients.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.scray.integration.ai.agent.AiIntegrationAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestClient {

	private final Logger logger = LoggerFactory.getLogger(AiIntegrationAgent.class);

	private URI url = null;

	public RestClient() {

		String scraySyncApiUrl = System.getenv("SCRAY_SYNC_API_URL");

		if (scraySyncApiUrl != null && !scraySyncApiUrl.isEmpty()) {

			try {
				this.url = new URI(scraySyncApiUrl);
			} catch (URISyntaxException e) {
				logger.warn("SCRAY_SYNC_API_URL is not a valid URL {}", url);
			}

		} else {
			try {
				this.url = new URI("http://localhost:8082");
			} catch (URISyntaxException e) {
				e.printStackTrace();
			}
		}
	}

	public String getData() throws IOException {
		String output = null;

		HttpURLConnection conn = (HttpURLConnection) url.resolve("sync/versioneddata/all/latest").toURL()
				.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP Error code : " + conn.getResponseCode());
		}
		InputStreamReader in = new InputStreamReader(conn.getInputStream());
		BufferedReader br = new BufferedReader(in);

		output = br.readLine();
		conn.disconnect();

		return output;
	}

	public void putData(String data) throws IOException {
		HttpURLConnection conn = (HttpURLConnection) url.resolve("/sync/versioneddata/latest").toURL().openConnection();
		conn.setRequestMethod("PUT");
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");
		conn.setDoOutput(true);

		try (OutputStream os = conn.getOutputStream()) {
			byte[] input = data.getBytes("utf-8");
			os.write(input, 0, input.length);
		} catch (Exception e) {
			e.printStackTrace();
		}

		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP Error code : " + conn.getResponseCode());
		}

		conn.disconnect();
	}

}
