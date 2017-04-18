package scray.cassandra.tools.types

import com.datastax.driver.core.Cluster
import scray.cassandra.tools.types.ScrayColumnType.ScrayColumnType
import com.datastax.driver.core.DataType

class CassandraColumnTypeReader(host: String, ks: String) {
  
  val metadata = Cluster.builder()
        .addContactPoint(host)
        .build().getMetadata.getKeyspace(ks);
  
  def getType(cf: String, column: String) = {
    val casType = metadata.getTable(cf).getColumn(column).getType
    findScrayType(column, casType)
  }
  
  def findScrayType(columnName: String, casType: DataType): ScrayColumnType = {
    casType.getName match {
      case DataType.Name.TEXT => scray.cassandra.tools.types.ScrayColumnType.String(columnName)
      case DataType.Name.BIGINT => scray.cassandra.tools.types.ScrayColumnType.Long(columnName)
    }
  }
  
}