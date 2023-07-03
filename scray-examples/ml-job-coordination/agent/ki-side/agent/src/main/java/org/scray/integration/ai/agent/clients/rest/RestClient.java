package org.scray.integration.ai.agent.clients.rest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class RestClient {

	public String getData() throws IOException {
		String output = null;

		URL url = new URL("http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata/all/latest");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
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
		URL url = new URL("http://ml-integration.research.dev.seeburger.de:8082/sync/versioneddata/latest");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("PUT");
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");
		conn.setDoOutput(true);
		System.out.println("puttt ");
		try(OutputStream os = conn.getOutputStream()) {
		    byte[] input = data.getBytes("utf-8");
		    os.write(input, 0, input.length);			
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		if (conn.getResponseCode() != 200) {
			throw new RuntimeException("Failed : HTTP Error code : " + conn.getResponseCode());
		}

		conn.disconnect();
	}

}
