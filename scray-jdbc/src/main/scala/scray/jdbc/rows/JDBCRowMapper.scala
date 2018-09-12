package scray.jdbc.rows

import java.sql.ResultSet
import scray.querying.description.Row
import java.sql.ResultSetMetaData
import scala.annotation.tailrec
import scray.querying.description.RowColumn
import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scray.querying.description.SimpleRow
import scala.collection.mutable.ArrayBuffer
import scray.querying.description.RowColumn
import scray.querying.description.TableIdentifier
import scray.querying.description.SimpleRow
import com.typesafe.scalalogging.LazyLogging
import scray.jdbc.extractors.JDBCSpecialColumnHandling

/**
 * simple Row-Mapper for JDBC columns from JDBC ResultSets 
 * TODO: use new type system for mappings
 */
class JDBCRowMapper(ti: TableIdentifier) extends Function1[ResultSet, Row] with LazyLogging {
  
  @tailrec private def walkColumns(rsMetadata: ResultSetMetaData, rs: ResultSet, counter: Int, colAgg: ArrayBuffer[RowColumn[_]]): ArrayBuffer[RowColumn[_]] = {
    def typesafeAdd[T](newValue: T): Unit = {
      colAgg += JDBCSpecialColumnHandling.getColumnFromValue(Column(rsMetadata.getColumnName(counter), ti), newValue)
    }
    
    if(counter > 0) {
      Option(rs.getObject(counter)).map { _ => 
        typesafeAdd(rs.getObject(counter))
      }
      walkColumns(rsMetadata, rs, counter - 1, colAgg)
    } else {
      colAgg
    }
  }
  
  override def apply(rs: ResultSet): Row = {
    val rsMetadata = rs.getMetaData
    val columns = walkColumns(rsMetadata, rs, rsMetadata.getColumnCount, new ArrayBuffer[RowColumn[_]])
    SimpleRow(columns)
  }
}