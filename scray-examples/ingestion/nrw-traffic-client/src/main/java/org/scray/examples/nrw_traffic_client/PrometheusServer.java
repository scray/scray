package org.scray.examples.nrw_traffic_client;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

public class PrometheusServer implements Runnable {

    @Override
    public void run() {
        DefaultExports.initialize();

        Server server = new Server(8080);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            System.out.println("Error while starting prometheus http server" + e);
            e.printStackTrace();
        }
        
    }

}
