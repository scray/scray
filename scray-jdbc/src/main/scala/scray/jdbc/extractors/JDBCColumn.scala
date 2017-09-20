package scray.jdbc.extractors

import scray.querying.description.TableIdentifier
import scray.querying.description.RowColumn
import scray.querying.description.TimestampColumn
import java.sql.Timestamp
import scray.querying.description.Column
import scray.querying.description.ColumnFactory
import java.lang.{Long => JLong, Integer => JInteger}
import scray.querying.description.ArrayByteColumn
import com.typesafe.scalalogging.LazyLogging
import java.sql.Blob

/**
 * case class which represents columns in JDBC databases
 */
case class JDBCColumn(name: String, ti: TableIdentifier, typ: Option[Int])

/**
 * transforms sql field values into RowColumns using the standard type
 * transformations from querying. If this cannot be applied invoke
 * special methods using reflection to create a similar column from the
 * value 
 */
object JDBCSpecialColumnHandling extends LazyLogging {
  
  def getColumnFromValue(col:Column, value: Any): RowColumn[_] = value match {
    case b: Blob =>
      new ArrayByteColumn(col, b.getBytes(1L, b.length().toInt))
    case o: Object if o.getClass.getName() == "oracle.sql.TIMESTAMP" =>
      // need special handling; do not want to make 
      // Oracle driver fixed dependency for maven, so we need reflection 
      val ts = o.getClass.getMethod("timestampValue", Array[Class[_]]():_*).invoke(o, Array[Object]():_*)
      new TimestampColumn(col, ts.asInstanceOf[Timestamp])
    case o: Object if o.getClass.getName() == "oracle.sql.BLOB" =>
      // need special handling; do not want to make 
      // Oracle driver fixed dependency for maven, so we need reflection 
      val len = o.getClass.getMethod("length", Array[Class[_]]():_*).invoke(o, Array[Object]():_*).asInstanceOf[JLong]
      logger.info("BLOB length: " + len)
      val ba = o.getClass.getMethod("getBytes", Array[Class[_]](classOf[JLong], classOf[JInteger]):_*).
        invoke(o, Array[Object](new JLong(1L), new JInteger(len.intValue())):_*) 
      new ArrayByteColumn(col, ba.asInstanceOf[Array[Byte]])
    case _ => ColumnFactory.getColumnFromValue(col, value)
  }
}