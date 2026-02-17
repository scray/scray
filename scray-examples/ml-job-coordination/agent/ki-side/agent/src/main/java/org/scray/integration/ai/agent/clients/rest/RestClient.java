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


import javax.net.ssl.*;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;


import org.scray.integration.ai.agent.AiIntegrationAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestClient {

	private final Logger logger = LoggerFactory.getLogger(AiIntegrationAgent.class);

	private URI url = null;

	private String TOKEN = "eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkphbmUgRG9lIiwiaWF0IjoxNzI5NDQwMDAwLCJleHAiOjE3Mjk0NDM2MDAsImlzcyI6ImF1dGguZXhhbXBsZS5jb20iLCJhdWQiOiJhcGkuZXhhbXBsZS5jb20iLCJzY29wZSI6InJlYWQ6dGhpbmdzIHdyaXRlOnRoaW5ncyJ9";
    private static void disableCertValidation() throws Exception {
        TrustManager[] trustAll = new TrustManager[] {
            new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }
        };
        SSLContext sc = SSLContext.getInstance("TLS");
        sc.init(null, trustAll, new SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

        // disable hostname verification
        HostnameVerifier allHostsValid = (hostname, session) -> true;
        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
    }



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



	// ------------------------------ HACK






	// ----------------------------------


	public String getData() throws IOException {
		String output = null;

		System.out.println(url);

        try {
			disableCertValidation(); // Hack
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // only for testing!
		HttpURLConnection conn = (HttpURLConnection) url.resolve("sync/versioneddata/all/latest").toURL()
				.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Accept", "application/json");
		conn.setRequestProperty("Authorization", "Bearer " + TOKEN);

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

        try {
			disableCertValidation(); // Hack
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // only for testing!

		HttpURLConnection conn = (HttpURLConnection) url.resolve("/sync/versioneddata/latest").toURL().openConnection();
		conn.setRequestMethod("PUT");
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");
		conn.setRequestProperty("Authorization", "Bearer " + TOKEN);
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
