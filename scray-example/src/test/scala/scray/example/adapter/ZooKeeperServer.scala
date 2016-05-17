package scray.example.adapter


import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
 
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import java.nio.file.Paths

class ZooKeeperServer() {

    val zkProperties: Properties = new Properties()
    val path = Paths.get(".").toAbsolutePath().normalize().toString() + "/target"
    zkProperties.put("dataDir", path)
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