package scray.cassandra.tools.types

import com.datastax.driver.core.Cluster
import scray.cassandra.tools.types.ScrayColumnTypes.ScrayColumnType
import com.datastax.driver.core.DataType
import com.datastax.driver.core.Metadata
import com.typesafe.scalalogging.slf4j.LazyLogging

class CassandraColumnTypeReader(host: String, ks: String) extends LazyLogging {

  val metadata = Cluster.builder()
    .addContactPoint(host)
    .build().getMetadata.getKeyspace(ks);

  def getType(cf: String, column: String) = {
    val casType = metadata.getTable(Metadata.quote(cf)).getColumn(Metadata.quote(column)).getType
    CassandraColumnTypeMapper.findScrayType(column, casType)
  }
}

object CassandraColumnTypeMapper extends LazyLogging {
  def findScrayType(columnName: String, casType: DataType): Option[ScrayColumnType] = {
    casType.getName match {
      case DataType.Name.TEXT    => Some(scray.cassandra.tools.types.ScrayColumnTypes.String(columnName))
      case DataType.Name.VARCHAR => Some(scray.cassandra.tools.types.ScrayColumnTypes.String(columnName))
      case DataType.Name.ASCII   => Some(scray.cassandra.tools.types.ScrayColumnTypes.String(columnName))
      case DataType.Name.BIGINT  => Some(scray.cassandra.tools.types.ScrayColumnTypes.Long(columnName))
      case DataType.Name.INT     => Some(scray.cassandra.tools.types.ScrayColumnTypes.Integer(columnName))
      case DataType.Name.BOOLEAN => Some(scray.cassandra.tools.types.ScrayColumnTypes.Boolean(columnName))
      case DataType.Name.DOUBLE  => Some(scray.cassandra.tools.types.ScrayColumnTypes.Double(columnName))
      case unknownCasType        => { logger.warn(s"No scray column type for ${unknownCasType} found."); None }
    }
  }
}