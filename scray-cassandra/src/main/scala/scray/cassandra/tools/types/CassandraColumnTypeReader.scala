package scray.cassandra.tools.types

import com.datastax.driver.core.Cluster
import scray.cassandra.tools.types.ScrayColumnTypes.ScrayColumnType
import com.datastax.driver.core.DataType
import com.datastax.driver.core.Metadata

class CassandraColumnTypeReader(host: String, ks: String) {
  
  val metadata = Cluster.builder()
        .addContactPoint(host)
        .build().getMetadata.getKeyspace(ks);
  
  def getType(cf: String, column: String) = {
    println(cf + "\t" + column)
    val casType = metadata.getTable(Metadata.quote(cf)).getColumn(Metadata.quote(column)).getType
    findScrayType(column, casType)
  }
  
  def findScrayType(columnName: String, casType: DataType): ScrayColumnType = {
    casType.getName match {
      case DataType.Name.TEXT => scray.cassandra.tools.types.ScrayColumnTypes.String(columnName)
      case DataType.Name.BIGINT => scray.cassandra.tools.types.ScrayColumnTypes.Long(columnName)
    }
  }
  
}