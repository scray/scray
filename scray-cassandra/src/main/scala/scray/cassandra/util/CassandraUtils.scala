package scray.cassandra.util

import com.datastax.driver.core.{KeyspaceMetadata, Metadata, TableMetadata}
import com.twitter.storehaus.cassandra.cql.CQLCassandraConfiguration.StoreColumnFamily
import org.yaml.snakeyaml.Yaml
import com.twitter.util.Try
import java.util.{Map => JMap, HashMap => JHashMap}

object CassandraUtils {

  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getKeyspaceMetadata(cf: StoreColumnFamily): KeyspaceMetadata = 
    cf.session.getSession.getCluster().getMetadata().getKeyspace(Metadata.quote(cf.session.getKeyspacename))
   
  /**
   * convenience method to retrieve KeyspaceMetadata from a StoreColumnFamily object
   */
  def getTableMetadata(cf: StoreColumnFamily, km: Option[KeyspaceMetadata] = None): TableMetadata = {
    val kspaceMeta = km match {
      case Some(ksm) => ksm
      case None => getKeyspaceMetadata(cf)
    }
    kspaceMeta.getTable(cf.getPreparedNamed)
  }
  
  /**
   * *not so fast* and *not thread-safe* method to write a property into the table comment of Cassandra.
   * If s.th. else is in the table comment, it will be overwritten.
   * Uses YAML to store properties in a string.
   * If you need synchronization please use external synchronization, e.g. Zookeeper.
   */
  def writeTablePropertyToCassandra(cf: StoreColumnFamily, property: String, value: String) = {
    // read
    val currentMap = getTablePropertiesFromCassandra(cf)
    // merge
    val map = currentMap match {
      case Some(properties) => 
        properties.put(property, value)
      case None =>
        val properties = new JHashMap[String, String]
        properties.put(property, value)
        properties
    }
    val yaml = new Yaml().dump(map)
    // write
    val cql = s"ALTER TABLE ${cf.getPreparedNamed} WITH comment='$yaml'"
    cf.session.getSession.execute(cql)
  }
  
  /**
   * *not so fast* method to read all table properties stored in a comment of a column family from Cassandra.
   * Uses YAML to store properties in a string.
   * If you need synchronization to sync with writers please use external synchronization, e.g. Zookeeper.
   */
  def getTablePropertiesFromCassandra(cf: StoreColumnFamily): Option[JMap[String, String]] = {
    val tableMeta = getTableMetadata(cf)
    val currentYaml = Option(tableMeta.getOptions.getComment)
    val yaml = new Yaml()
    currentYaml.flatMap { content =>
      Try {
        yaml.load(content).asInstanceOf[JMap[String, String]]
      }.toOption
    }    
  } 
  
  /**
   * *not so fast* method to read a table property stored in a comment of a column family from Cassandra.
   * Uses YAML to store properties in a string.
   * If you need synchronization to sync with writers please use external synchronization, e.g. Zookeeper.
   */
  def getTablePropertyFromCassandra(cf: StoreColumnFamily, property: String): Option[String] = 
    getTablePropertiesFromCassandra(cf).flatMap(map => Option(map.get(property)))
}