package scray.jdbc.rows

import java.sql.ResultSet
import scray.querying.description.Row
import scala.collection.mutable.HashSet
import scray.querying.description.Column
import scray.querying.description.TableIdentifier
import java.sql.ResultSetMetaData
import scray.querying.description.SimpleRow
import scray.querying.description.RowColumnComparator
import scray.querying.description.RowColumn
import scala.collection.mutable.ArrayBuffer



/**
 * This class simulates a single Row in a JDBC context.
 * However, in JDBC there are no rows. So we expect
 * ResultSet to be set correctly and delegate calls to
 * the corresponding ResultSet methods.
 * The advantage of this approach is that we don't need
 * to convert to Scray rows as we can just extend the 
 * Row trait.
 */
class JDBCRow(rSet: ResultSet, dbSystem: String, dbId: String,
    meta: ResultSetMetaData, columns: List[Column], count: Int) extends Row {
  
  val values = (1 to count).map { pos =>
    rSet.getObject(pos)
  }
  val namedValues = columns.map { col =>
    (col.columnName, Option(rSet.getObject(col.columnName)))
  }.filter(_._2.isDefined).toMap
  
  override def getColumnValue[V](colNum: Int): Option[V] =
    Option(values(colNum)).asInstanceOf[Option[V]]
  override def getColumnValue[V](col: Column): Option[V] = {
    // we assume dbSystem and tableId are correct to spare
    // Scray from massively testing Strings for identity
    Option(namedValues.get(col.columnName)).asInstanceOf[Option[V]]
  }
  override def getColumns: List[Column] = columns 
  override def getNumberOfEntries: Int = count
  @inline override def intersectValues(cols: HashSet[Column]): Row = {
    val rowCols = new ArrayBuffer ++ getColumns.map { col => RowColumn(col, getColumnValue(col).get) }
    new SimpleRow(RowColumnComparator.intersectValues(rowCols.asInstanceOf[ArrayBuffer[RowColumn[_]]], cols))
  }
  /**
   * recalculating meta information doesn't make sense in a JDBC context,
   * because each row has the same metadata for a ResultSet
   */
  @inline override def recalculateInternalCaches: Unit = Unit

}