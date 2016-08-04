// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.querying.description

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe.typeTag
import scala.collection.mutable.HashSet
import com.twitter.util.Try



/**
 * A Row in the result set representing a data record. 
 * Column value reference are starting at 0, inclusive.
 * Size can vary from Row to Row.
 */
trait Row {
  def getColumnValue[V](colNum: Int): Option[V]
  def getColumnValue[V](col: Column): Option[V]
  def getColumns: List[Column]
  def getNumberOfEntries: Int
  def isEmpty = getNumberOfEntries == 0
  @inline def intersectValues(cols: HashSet[Column]): Row
  @inline def recalculateInternalCaches: Unit
}

case class RowColumn[V](column: Column, value: V)//(implicit val valuesType: TypeTag[V])

object RowColumnComparator {
  def apply(a: Column, b: Column): Boolean = compare(a, b)
  def compare(a: Column, b: Column): Boolean = {
    val coln = a.columnName.compareTo(b.columnName)
    if(coln == 0) {
      val tabn = a.table.tableId.compareTo(b.table.tableId)
      if(tabn == 0) {
        val sdn = a.table.dbId.compareTo(b.table.dbId)
        if(sdn == 0) {
          val dbs = a.table.dbSystem.compareTo(b.table.dbSystem)
          dbs >= 0
        } else {
          sdn > 0
        }
      } else {
        tabn > 0
      }
    } else {
      coln > 0
    }
  }
  val compFnColumns: (Column, Column) => Boolean = compare(_, _)
  val compFnRowColumns: (RowColumn[_], RowColumn[_]) => Boolean = (a, b) => compare(a.column, b.column)

  /**
   * this needs to be real fast, as it is used in the
   * innermost loop, the ColumnDispenserSource
   */
  @inline def intersectValues(columns: ArrayBuffer[RowColumn[_]], intersector: HashSet[Column]): ArrayBuffer[RowColumn[_]] = {
    val resultCollection = new ArrayBuffer[RowColumn[_]]
    columns.foreach(rowcol => if(intersector.contains(rowcol.column)) { resultCollection += rowcol })
    resultCollection
  }
}

/**
 * case class representing a row; memoizes results on demand
 */
case class SimpleRow(
  columns: ArrayBuffer[RowColumn[_]]
) extends Row {
  @transient var cols: Map[Column, RowColumn[_]] = Map()
  @transient var columnList: List[Column] = List()
  recalculateInternalCaches
  
  override def getColumnValue[V](colNum: Int): Option[V] = Try(columns(colNum).value).toOption.asInstanceOf[Option[V]]
  
  override def getColumnValue[V](col: Column): Option[V] = cols.get(col).map(_.value.asInstanceOf[V])
  
  override def getColumns: List[Column] = columnList
  
  override def getNumberOfEntries: Int = columns.size
  
  @inline override def intersectValues(cols: HashSet[Column]): Row = {
    val resultCollection = RowColumnComparator.intersectValues(columns, cols)
    columns.clear
    columns ++= resultCollection
    recalculateInternalCaches
    this
  }
  
  @inline override def recalculateInternalCaches: Unit = {
    columnList = columns.map(_.column).toList
    cols = columns.map(c => (c.column, c)).toMap
  }
}

/**
 * Composes a List of Rows into a single Row, i.e. useful 
 * for result transformation. The order of the rows in the list 
 * is important for the reference numbers of the columns.
 */
class CompositeRow(val rows: List[Row]) extends Row {
  override def getColumnValue[V](colNum: Int): Option[V] = {
    @tailrec def getRelevantRowForEntryNumber(colNumLocal: Int, rowList: List[Row]): Option[V] = {
      val size = rowList.head.getNumberOfEntries
      if(colNumLocal < size) { rowList.head.getColumnValue(colNumLocal) }
      else { getRelevantRowForEntryNumber(colNumLocal - size, rowList.tail) }
    }
    getRelevantRowForEntryNumber(colNum, rows)
  }
  override def getColumnValue[V](col: Column): Option[V] = rows.collect {
      case row: Row if row.getColumnValue(col).isDefined => row.getColumnValue(col)
    }.headOption.flatten
    
  override def getNumberOfEntries: Int = rows.foldLeft(0)(_ + _.getNumberOfEntries)
  override def getColumns: List[Column] = rows.foldLeft(List[Column]())((agg, row) => agg ++ row.getColumns)
  @inline override def intersectValues(cols: HashSet[Column]): Row = {
    rows.foreach(_.intersectValues(cols))
    this
  }
  @inline override def recalculateInternalCaches: Unit = rows.foreach(_.recalculateInternalCaches)
  override def toString: String = s"CompositeRow(${rows.mkString(",")})"
}

/**
 * A row with no data. Used for filtering.
 */
class EmptyRow extends Row with Serializable {
  override def getColumnValue[V](colNum: Int): Option[V] = None
  override def getColumnValue[V](col: Column): Option[V] = None
  override def getColumns: List[Column] = List()
  override def getNumberOfEntries: Int = 0
  @inline override def intersectValues(cols: HashSet[Column]): Row = this
  @inline override def recalculateInternalCaches: Unit = {}
}
