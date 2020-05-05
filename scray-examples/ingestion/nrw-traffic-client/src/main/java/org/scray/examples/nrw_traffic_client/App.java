package org.scray.examples.nrw_traffic_client;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import io.prometheus.client.Counter;
import io.swagger.client.ApiClient;
import io.swagger.client.ApiException;
import io.swagger.client.api.AdminsApi;

public class App {
    public static void main(String[] args) {
        final int POLLING_INTERVAL = 60000;
        HttpClient client = new HttpClient();
        D2LogicalModelStringMapper mapper = new D2LogicalModelStringMapper();
        Thread pServer = null;
        
        final Counter nrwHttpRequests = Counter.build()
                .name("nrw_traffic_client_http_requests")
                .help("Total requests.")
                .register();
        pServer = new Thread(new PrometheusServer());
        pServer.start();

        while (!Thread.interrupted()) {
            try {
                
                List<String> jsonData = client.getData(
                        "http://datarun2018.de/BASt-MDM-Interface/srv/2865003/clientPullService?subscriptionID=2865003",
                        mapper);
                System.out.println("New data: " + jsonData.get(0).substring(0, 200));

                AdminsApi apiInstance = new AdminsApi();

                ApiClient apiClient = apiInstance.getApiClient();
                URL url = new URL(apiClient.getBasePath());
                URL newUrl = new URL(url.getProtocol(), "127.0.0.1", 8080, url.getFile());
                apiClient.setBasePath(newUrl.toString());

                try {
                    System.out.println("Insert " + jsonData.size() + " json object(s)");
                    nrwHttpRequests.inc(); // Increment counter
                    for (String elaboratedData : jsonData) {
                        apiInstance.insert("nrw", "traffic", elaboratedData);
                    }
                    
                } catch (ApiException e) {
                    System.err.println("Exception when calling AdminsApi#insert. Retry in " + POLLING_INTERVAL + "ms");
                }

            } catch (UnsupportedOperationException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
