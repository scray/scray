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
import scala.reflect.runtime.universe.{TypeTag, typeTag}
import scala.collection.immutable.HashMap

/**
 * A Row in the result set representing a data record. 
 * Column value reference are starting at 0, inclusive.
 * Size can vary from Row to Row.
 */
trait Row {
  def getColumnValue[V](colNum: Int): Option[V]
  def getColumnValue[V](col: Column): Option[V]
  def getColumnValueType(colNum: Int): Option[TypeTag[_]]
  def getColumnValueType(col: Column): Option[TypeTag[_]]
  def getColumns: List[Column]
  def getNumberOfEntries: Int
  def isEmpty = getNumberOfEntries == 0
}

case class RowColumn[V](column: Column, value: V)(implicit val valuesType: TypeTag[V])

/**
 * case class representing a row; memoizes results on demand
 */
case class SimpleRow(
  columns: List[RowColumn[_]]
) extends Row {
  override def getColumnValue[V](colNum: Int): Option[V] = columns.lift(colNum).map(_.value.asInstanceOf[V])
  override def getColumnValueType(colNum: Int): Option[TypeTag[_]] = columns.lift(colNum).map(_.valuesType)

  lazy val cols: Map[Column, RowColumn[_]] = columns.map(c => (c.column, c)).toMap
  override def getColumnValue[V](col: Column): Option[V] = cols.get(col).map(_.value.asInstanceOf[V])
  override def getColumnValueType(col: Column): Option[TypeTag[_]] = cols.get(col).map(_.valuesType)
  
  lazy val columnList: List[Column] = columns.map(_.column)
  override def getColumns: List[Column] = columnList
  
  lazy val size = columns.size
  override def getNumberOfEntries: Int = size 
}

/**
 * Composes a List of Rows into a single Row, i.e. useful 
 * for result transformation. The order of the rows in the list 
 * is important for the reference numbers of the columns.
 */
class CompositeRow(rows: List[Row]) extends Row {
  override def getColumnValue[V](colNum: Int): Option[V] = {
    @tailrec def getRelevantRowForEntryNumber(colNumLocal: Int, rowList: List[Row]): Option[V] = {
      val size = rowList.head.getNumberOfEntries
      if(colNumLocal < size) { rowList.head.getColumnValue(colNumLocal) }
      else { getRelevantRowForEntryNumber(colNumLocal - size, rowList.tail) }
    }
    getRelevantRowForEntryNumber(colNum, rows)
  }
  override def getColumnValue[V](col: Column): Option[V] = {
    rows.find(row => row.getColumns.contains(col)).flatMap(_.getColumnValue(col))
  }
  override def getColumnValueType(colNum: Int): Option[TypeTag[_]] = {
    @tailrec def getRelevantRowForEntryNumber(colNumLocal: Int, rowList: List[Row]): Option[TypeTag[_]] = {
      val size = rowList.head.getNumberOfEntries
      if(colNumLocal < size) { rowList.head.getColumnValueType(colNumLocal) }
      else { getRelevantRowForEntryNumber(colNumLocal - size, rowList.tail) }
    }
    getRelevantRowForEntryNumber(colNum, rows)
  }
  override def getColumnValueType(col: Column): Option[TypeTag[_]] = {
    rows.find(row => row.getColumns.contains(col)).flatMap(_.getColumnValueType(col))
  }
  override def getNumberOfEntries: Int = rows.foldLeft(0)(_ + _.getNumberOfEntries)
  override def getColumns: List[Column] = rows.foldLeft(List[Column]())((agg, row) => agg ++ row.getColumns)
}

/**
 * A row with no data. Used for filtering.
 */
class EmptyRow extends Row with Serializable {
  override def getColumnValue[V](colNum: Int): Option[V] = None
  override def getColumnValue[V](col: Column): Option[V] = None
  override def getColumnValueType(colNum: Int): Option[TypeTag[_]] = None
  override def getColumnValueType(col: Column): Option[TypeTag[_]] = None
  override def getColumns: List[Column] = List()
  override def getNumberOfEntries: Int = 0
}
