package scray.jdbc.rows

import java.sql.ResultSet
import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import com.twitter.util.Closable
import com.twitter.util.Future
import com.twitter.util.Time

/**
 * some Iterator to retrieve JDBC rows, which are also Scray rows
 */
class JDBCRowIterator(rSet: ResultSet, dbSystem: String, dbId: String) extends Iterator[JDBCRow] with Closable {
  
  // pre-calculate meta-information
  val meta = rSet.getMetaData
  val count = meta.getColumnCount
  val columns: List[Column] = {
    (1.to(count)).map { pos =>
      val ti = TableIdentifier(dbSystem, dbId, meta.getTableName((pos)))
      Column(meta.getColumnName(pos), ti)
    }.toList
  }
  
  override def hasNext: Boolean = !(rSet.isClosed() || rSet.isLast() || rSet.isAfterLast())
  def next(): JDBCRow = {
    rSet.next()
    new JDBCRow(rSet, dbSystem, dbId, meta, columns, count)
  }
  
  override def close(time: Time) = Future.value(rSet.close())
}