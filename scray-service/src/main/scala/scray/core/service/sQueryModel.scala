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
package scray.core.service

import java.util.UUID

import scala.util.{ Failure, Success, Try }
import scala.math.ScalaNumericAnyConversions

import org.parboiled2.CharPredicate
import org.parboiled2.Parser
import org.parboiled2.ParserInput
import org.parboiled2.ParserInput.apply
import org.parboiled2.Rule0
import org.parboiled2.Rule1
import org.parboiled2.Rule2

import scray.common.exceptions.ExceptionIDs
import scray.common.exceptions.ScrayServiceException
import scray.common.serialization.StringLiteralDeserializer

import scray.querying.Query
import scray.querying.queries.SimpleQuery
import scray.querying.description._

import scray.service.qmodel.thrifscala.ScrayTColumnInfo
import scray.service.qmodel.thrifscala.ScrayTQuery

trait _QueryComponent
 
case class _Query(components : Map[Class[_ <: _QueryComponent], _QueryComponent])(implicit val tQuery : ScrayTQuery) {
 
  def getColumns() : _Columns = components.get(classOf[_Columns]).get.asInstanceOf[_Columns]
  def getTable() : _Table = components.get(classOf[_Table]).get.asInstanceOf[_Table]
  def getPredicate() : Option[_Predicate] = components.get(classOf[_Predicate]).map(_.asInstanceOf[_Predicate])
  def getOrdering() : Option[_Ordering] = components.get(classOf[_Ordering]).map(_.asInstanceOf[_Ordering])
  def getGrouping() : Option[_Grouping] = components.get(classOf[_Grouping]).map(_.asInstanceOf[_Grouping])
  def getRange() : Option[_Range] = components.get(classOf[_Range]).map(_.asInstanceOf[_Range])
 
  // query generator
  def createQuery() : Try[Query] = Try {
    SimpleQuery(
      space = tQuery.queryInfo.querySpace,
      table = getTableIdentifier(),
      id = UUID.randomUUID,
      columns = getColumns.generate()(this),
      where = getPredicate().map { _.generate()(this) },
      grouping = getGrouping().map { _.generate()(this) },
      ordering = getOrdering().map { _.generate()(this) },
      range = getRange().map { _.generate })
  }
 
  // consecutive builder functions
 
  /**
   * - need to check thrift references (ok)
   * - need to check for duplicate columns (ok)
   * - need to check thrift references (ok)
   */
  def addColumns(columns : _Columns) : Try[_Query] =
    Try(columns match {
      case ac : _AsterixColumn => _Query(components + (classOf[_Columns] -> columns))
      case cs : _ColumnSet => _Query(components + (classOf[_Columns] -> {
        // all elements need to be type equal (just RefColumns or just SpecColumns)
        cs.components.reduceLeft((l, r) =>
          if (l.getClass().equals(r.getClass())) { l } else {
            throw new ScrayServiceException(
              ExceptionIDs.PARSING_ERROR,
              query = None,
              s"Inconsistent columns (all columns need to be either references or specfications).",
              cause = None)
          })
        // no duplicate columns allowed
        if (cs.components.removeDuplicates.size == components.size) {
          throw new ScrayServiceException(
            ExceptionIDs.PARSING_ERROR,
            query = None,
            s"Duplicated column names in FROM part.",
            cause = None)
        }
        // check for thrift references in ref columns
        if (!cs.components.filter(_.isInstanceOf[_RefColumn])
          .foldLeft(true)((a, b) => a && tQuery.queryInfo.columns.find(_.name.equals(b.getName)).nonEmpty)) {
          throw new ScrayServiceException(
            ExceptionIDs.PARSING_ERROR,
            query = None,
            s"Unmatched column reference in FROM part.",
            cause = None)
        }
        cs
      }))
    })
 
  def addTable(table : _Table) : Try[_Query] = Try(_Query(components + (classOf[_Table] -> table)))
 
  def addRange(range : Option[_Range]) : Try[_Query] = Try(if (range.isEmpty) { this } else {
    _Query(components + (classOf[_Range] -> range.get))
  })
 
  /**
   * - need to check column references (ok)
   * - need to check value types (todo)
   */
  def addPredicate(predicate : Option[_Predicate]) : Try[_Query] = Try(predicate match {
    case Some(p) => _Query(components + (classOf[_Predicate] -> p.getMatched(getColumns).get))
    case _ => this
  })
 
  /**
   * - need to check column references (ok)
   */
  def addGrouping(grouping : Option[_Grouping]) : Try[_Query] =
    Try(if (grouping.isEmpty) { this } else {
      if (hasColumn(grouping.get.name)) {
        _Query(components + (classOf[_Grouping] -> grouping.get))
      } else {
        throw new ScrayServiceException(
          ExceptionIDs.PARSING_ERROR,
          query = None,
          s"Grouping contains unmatched column name '${grouping.get.name}'.",
          cause = None)
      }
    })
 
  /**
   * - need to check column references (ok)
   */
  def addOrdering(ordering : Option[_Ordering]) : Try[_Query] =
    Try(if (ordering.isEmpty) { this } else {
      if (hasColumn(ordering.get.name)) {
        _Query(components + (classOf[_Ordering] -> ordering.get))
      } else {
        throw new ScrayServiceException(
          ExceptionIDs.PARSING_ERROR,
          query = None,
          s"Ordering contains unmatched column name '${ordering.get.name}'.",
          cause = None)
      }
    })
 
  def getTableIdentifier() : TableIdentifier = getTable match {
    case rt : _RefTable => rt.generate()(this).get
    case st : _SpecTable => st.generate
  }
 
  def hasColumn(name : String) : Boolean = getColumn(name).nonEmpty
  def getColumn(name : String) : Option[_Column] = getColumns.find(name)
 
}
 
/**
 * Column collection type
 */
trait _Columns extends _QueryComponent {
  def isContained(cName : String) : Boolean
  def find(cName : String) : Option[_Column]
  def generate()(implicit _Query : _Query) : Columns
}
case class _ColumnSet(components : List[_Column]) extends _Columns {
  override def isContained(cName : String) : Boolean = find(cName).nonEmpty
  override def find(cName : String) : Option[_Column] = components.find(_.getName.equals(cName))
  override def generate()(implicit _Query : _Query) = Columns(Right(components.map { _.generate } toList))
}
case class _AsterixColumn extends _Columns {
  override def isContained(cName : String) : Boolean = true
  override def find(cName : String) : Option[_Column] = Some(_SpecColumn(cName, None))
  override def generate()(implicit _Query : _Query) = Columns(Left(true))
}
 
/**
 * Column type responsible for explicit ref matching
 */
trait _Column extends _QueryComponent {
  def getName : String
  def generate()(implicit query : _Query) : Column = Column(getName, query.getTableIdentifier)
}
 
/**
 * Column defined as string literal
 */
case class _SpecColumn(name : String, typeTag : Option[String]) extends _Column {
  override def getName = name
}
 
/**
 * Column defined as thrift reference
 */
case class _RefColumn(reference : String) extends _Column {
  override def getName = reference
 
  /**
   * Provides type information as far as possible.
   * will return 1. class name (if exists) or 2. type tag (if exists) or 3. None
   *
   * @param query context
   * @return type optional type information string
   */
  def getType()(implicit query : _Query) : Option[String] = getThriftColInfo.map {
    (cInf : ScrayTColumnInfo) => cInf.tType.get.className.getOrElse(cInf.tType.get.tType.name)
  }.toOption
 
  private def getThriftColInfo()(implicit query : _Query) : Try[ScrayTColumnInfo] = Try(query.tQuery.queryInfo.columns
    .find(col => col.name.equals(reference))
    .getOrElse(throw new ScrayServiceException(
      ExceptionIDs.PARSING_ERROR,
      query = None,
      s"Unmatched column reference '$getName'.",
      cause = None)))
}
 
/**
 * PostPredicate type
 */
trait _PostPredicate extends _QueryComponent
 
case class _Ordering(name : String) extends _PostPredicate {
  def generate()(implicit query : _Query) : ColumnOrdering[_] = ColumnOrdering(query.getColumn(name).get.generate)(ordered)
}
 
case class _Grouping(name : String) extends _PostPredicate {
  def generate()(implicit query : _Query) : ColumnGrouping = ColumnGrouping(query.getColumn(name).get.generate)
}
 
case class _Range(skip : Option[String], limit : Option[String]) extends _QueryComponent {
  def generate() = QueryRange(skip = skip.map { _.toLong }, limit = limit.map { _.toLong })
}
 
/**
 * Table type
 *
 * Tables can be specified by means of a literal or thrift reference.
 *
 */
trait _Table extends _QueryComponent
 
case class _SpecTable(dbSystem : String, dbId : String, tabId : String) extends _Table {
  def generate() : TableIdentifier = TableIdentifier(dbSystem, dbId, tabId)
}
 
case class _RefTable(reference : String) extends _Table {
  def generate()(implicit query : _Query) : Try[TableIdentifier] = Try(asSpecTable.get.generate)
  def asSpecTable()(implicit query : _Query) : Try[_SpecTable] = {
    val ttab = query.tQuery.queryInfo.tableInfo
    if (ttab.tableId.equals(reference)) {
      Success(_SpecTable(ttab.dbSystem, ttab.dbId, ttab.tableId))
    } else {
      Failure(new ScrayServiceException(
        ExceptionIDs.PARSING_ERROR,
        query = None,
        s"Unmatched table reference '$reference'.",
        cause = None))
    }
  }
}
 
/**
 * Predicate type
 */
trait _Predicate extends _QueryComponent {
 
  /**
   * Explicit column matching
   * @param columns set of columns to check against
   * @return the predicate if success, exception otherwise
   */
  def getMatched(columns : _Columns) : Try[_Predicate]
 
  /**
   * Generates equivalent query clause
   * @param query query context
   * @return the query clause
   */
  def generate()(implicit query : _Query) : Clause
}
 
/**
 * Predicates containing subpredicates
 */
abstract class _ComplexPredicate(subpredicates : List[_Predicate]) extends _Predicate
 
/**
 * AND predicate
 */
case class _And(subpredicates : List[_Predicate]) extends _ComplexPredicate(subpredicates) {
  override def generate()(implicit query : _Query) = And(subpredicates.map(_.generate) : _*)
  override def getMatched(columns : _Columns) = Try { _And(subpredicates.map { _.getMatched(columns).get }) }
}
 
/**
 * OR predicate
 */
case class _Or(subpredicates : List[_Predicate]) extends _ComplexPredicate(subpredicates) {
  override def generate()(implicit query : _Query) = { Or(subpredicates.map(_.generate) : _*) }
  override def getMatched(columns : _Columns) = Try { _Or(subpredicates.map { _.getMatched(columns).get }) }
}
 
/**
 * Atomic predicate type
 */
abstract class _AtomicPredicate(columnName : String, value : _Value) extends _Predicate {
 
  override def getMatched(columns : _Columns) : Try[_Predicate] = if (isMatchedBy(columns)) Success(this) else
    Failure(new ScrayServiceException(
      ExceptionIDs.PARSING_ERROR,
      query = None,
      s"Predicate contains unmatched column name '${columnName}'.",
      cause = None))
 
  def isMatchedBy(columns : _Columns) : Boolean = columns match {
    case ac : _AsterixColumn => true
    case cs : _ColumnSet => cs.components.find(_.getName.equals(columnName)).nonEmpty
  }
 
  def getColumn()(implicit query : _Query) : Column = query.getColumn(columnName).get.generate
  def getValue()(implicit query : _Query) : Try[Comparable[_]] = Try {
    val aval = value match {
      case lv : _LitVal => lv.deserializeLiteral.get
      case rv : _RefVal => rv.deserializeValue()(query.tQuery).get
    }
    aval.asInstanceOf[Comparable[_]]
  }
}
 
/*
 * Concrete atomic predicates (=,<,>,<=,>=) containing individual generators
 */
 
case class _Equal(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value) {
  def generate()(implicit query : _Query) : Equal[Ordered[_]] = Equal[Ordered[_]](getColumn, getValue.get)
}
case class _Greater(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value) {
  def generate()(implicit query : _Query) : Greater[Ordered[_]] = Greater[Ordered[_]](getColumn, getValue.get)
}
case class _GreaterEqual(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value) {
  def generate()(implicit query : _Query) : GreaterEqual[Ordered[_]] = GreaterEqual[Ordered[_]](getColumn, getValue.get)
}
case class _Smaller(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value) {
  def generate()(implicit query : _Query) : Smaller[Ordered[_]] = Smaller[Ordered[_]](getColumn, getValue.get)
}
case class _SmallerEqual(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value) {
  def generate()(implicit query : _Query) : SmallerEqual[Ordered[_]] = SmallerEqual[Ordered[_]](getColumn, getValue.get)
}
 
/**
 * Value types
 */
trait _Value
 
/**
 * Adapter for literal parser
 */
case class _LitVal(literal : String, typeTag : Option[String]) extends _Value {
  def deserializeLiteral() : Try[_] = typeTag match {
    case Some(typeTag) => StringLiteralDeserializer.deserialize(literal, typeTag)
    case None => StringLiteralDeserializer.deserialize(literal)
  }
  def typeCheck(typName : String) : Option[Boolean] = Option(typeTag.getOrElse(None).equals(typName))
}
 
/**
 * Adapter for deserializer framework
 */
case class _RefVal(ref : String) extends _Value {
  def deserializeValue()(implicit tQuery : ScrayTQuery) : Try[Any] = throw new NotImplementedError
  def typeCheck()(implicit tQuery : ScrayTQuery) : Option[Boolean] = throw new NotImplementedError
}