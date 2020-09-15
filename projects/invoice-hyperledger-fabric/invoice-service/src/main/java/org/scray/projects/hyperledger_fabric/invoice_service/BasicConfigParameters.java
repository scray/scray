package org.scray.projects.hyperledger_fabric.invoice_service;

import java.io.File;
import java.nio.file.Paths;

public class BasicConfigParameters {
	
	private String networkConfigPath = Paths.get(".").toAbsolutePath().normalize().toString() + File.separator + "config" + File.separator + "connection-org1.yaml";
	private String caCertPem = Paths.get(".").toAbsolutePath().normalize().toString() + File.separator + "config" + File.separator + "ca.org1.example.com-cert.pem";
	private String hyperlederHost = "localhost";
	    
    public String getHyperlederHost() {
        return hyperlederHost;
    }
    public void setHyperlederHost(String hyperlederHost) {
        this.hyperlederHost = hyperlederHost;
    }
    public String getNetworkConfigPath() {
        return networkConfigPath;
    }
    public void setNetworkConfigPath(String networkConfigPath) {
        this.networkConfigPath = networkConfigPath;
    }
    public String getCaCertPem() {
        return caCertPem;
    }
    public void setCaCertPem(String caCertPem) {
        this.caCertPem = caCertPem;
    }
    @Override
    public String toString() {
        return "BasicConfigParameters [networkConfigPath=" + networkConfigPath + ", caCertPem=" + caCertPem + "]";
    }
    
    

}
