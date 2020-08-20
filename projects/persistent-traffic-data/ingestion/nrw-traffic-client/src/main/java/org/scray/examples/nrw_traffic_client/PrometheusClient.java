package org.scray.examples.nrw_traffic_client;

import java.io.IOException;

import io.prometheus.client.Counter;
import io.prometheus.client.hotspot.DefaultExports;

public class PrometheusClient {


    
    public static void main(String[] args) throws InterruptedException, IOException {
        PrometheusServer pServer = new PrometheusServer();
        
        (new Thread(pServer)).start();
        
        Thread.sleep(5000);
    }

}
