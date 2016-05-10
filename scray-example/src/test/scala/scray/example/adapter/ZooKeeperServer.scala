package scray.example.adapter


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
 
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

class ZooKeeperServer() {

    val zkProperties: Properties = new Properties()
    zkProperties.put("dataDir", "/tmp")
    zkProperties.put("clientPort", "12345")
    
		val quorumConfiguration = new QuorumPeerConfig()
    quorumConfiguration.parseProperties(zkProperties)

		val zooKeeperServer = new ZooKeeperServerMain()
		val configuration = new ServerConfig()
		configuration.readFrom(quorumConfiguration);
		
		
		new Thread(new Runnable {
		    def run() {zooKeeperServer.runFromConfig(configuration)}
		}).start()
}