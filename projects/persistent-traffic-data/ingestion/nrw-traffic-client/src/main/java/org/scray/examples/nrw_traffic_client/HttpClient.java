package org.scray.examples.nrw_traffic_client;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class HttpClient {

    public List<String> getData(String url, D2LogicalModelStringMapper converter)
            throws UnsupportedOperationException, IOException {
        List<String> jsons = null;

        CloseableHttpClient client = HttpClients.createDefault();

        HttpGet method = new HttpGet(url);
        CloseableHttpResponse response = client.execute(method);

        HttpEntity entity = response.getEntity();
        if (entity != null) {
            InputStream instream = entity.getContent();
            jsons = converter.convertXmlToJson(instream);
        }

        response.close();
        return jsons;
    }

}
