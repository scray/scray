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

trait SQueryComponent

case class SQuery(components : Map[Class[_ <: SQueryComponent], SQueryComponent])(implicit val tQuery : ScrayTQuery) {

  def getColumns() : SColumns = components.get(classOf[SColumns]).get.asInstanceOf[SColumns]
  def getTable() : STable = components.get(classOf[STable]).get.asInstanceOf[STable]
  def getPredicate() : Option[SPredicate] = components.get(classOf[SPredicate]).map(_.asInstanceOf[SPredicate])
  def getOrdering() : Option[SOrdering] = components.get(classOf[SOrdering]).map(_.asInstanceOf[SOrdering])
  def getGrouping() : Option[SGrouping] = components.get(classOf[SGrouping]).map(_.asInstanceOf[SGrouping])
  def getRange() : Option[SRange] = components.get(classOf[SRange]).map(_.asInstanceOf[SRange])

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
  def addColumns(columns : SColumns) : Try[SQuery] =
    Try(columns match {
      case ac : SAsterixColumn => SQuery(components + (classOf[SColumns] -> columns))
      case cs : SColumnSet => SQuery(components + (classOf[SColumns] -> {
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
        if (!cs.components.filter(_.isInstanceOf[SRefColumn])
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

  def addTable(table : STable) : Try[SQuery] = Try(SQuery(components + (classOf[STable] -> table)))

  def addRange(range : Option[SRange]) : Try[SQuery] = Try(if (range.isEmpty) { this } else {
    SQuery(components + (classOf[SRange] -> range.get))
  })

  /**
   * - need to check column references (ok)
   * - need to check value types (todo)
   */
  def addPredicate(predicate : Option[SPredicate]) : Try[SQuery] = Try(predicate match {
    case Some(p) => SQuery(components + (classOf[SPredicate] -> p.getMatched(getColumns).get))
    case _ => this
  })

  /**
   * - need to check column references (ok)
   */
  def addGrouping(grouping : Option[SGrouping]) : Try[SQuery] =
    Try(if (grouping.isEmpty) { this } else {
      if (hasColumn(grouping.get.name)) {
        SQuery(components + (classOf[SGrouping] -> grouping.get))
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
  def addOrdering(ordering : Option[SOrdering]) : Try[SQuery] =
    Try(if (ordering.isEmpty) { this } else {
      if (hasColumn(ordering.get.name)) {
        SQuery(components + (classOf[SOrdering] -> ordering.get))
      } else {
        throw new ScrayServiceException(
          ExceptionIDs.PARSING_ERROR,
          query = None,
          s"Ordering contains unmatched column name '${ordering.get.name}'.",
          cause = None)
      }
    })

  def getTableIdentifier() : TableIdentifier = getTable match {
    case rt : SRefTable => rt.generate()(this).get
    case st : SSpecTable => st.generate
  }

  def hasColumn(name : String) : Boolean = getColumn(name).nonEmpty
  def getColumn(name : String) : Option[SColumn] = getColumns.find(name)

}

/**
 * Column collection type
 */
trait SColumns extends SQueryComponent {
  def isContained(cName : String) : Boolean
  def find(cName : String) : Option[SColumn]
  def generate()(implicit SQuery : SQuery) : Columns
}
case class SColumnSet(components : List[SColumn]) extends SColumns {
  override def isContained(cName : String) : Boolean = find(cName).nonEmpty
  override def find(cName : String) : Option[SColumn] = components.find(_.getName.equals(cName))
  override def generate()(implicit SQuery : SQuery) = Columns(Right(components.map { _.generate } toList))
}
case class SAsterixColumn extends SColumns {
  override def isContained(cName : String) : Boolean = true
  override def find(cName : String) : Option[SColumn] = Some(SSpecColumn(cName, None))
  override def generate()(implicit SQuery : SQuery) = Columns(Left(true))
}

/**
 * Column type responsible for explicit ref matching
 */
trait SColumn extends SQueryComponent {
  def getName : String
  def generate()(implicit query : SQuery) : Column = Column(getName, query.getTableIdentifier)
}

/**
 * Column defined as string literal
 */
case class SSpecColumn(name : String, typeTag : Option[String]) extends SColumn {
  override def getName = name
}

/**
 * Column defined as thrift reference
 */
case class SRefColumn(reference : String) extends SColumn {
  override def getName = reference

  /**
   * Provides type information as far as possible.
   * will return 1. class name (if exists) or 2. type tag (if exists) or 3. None
   *
   * @param query context
   * @return type optional type information string
   */
  def getType()(implicit query : SQuery) : Option[String] = getThriftColInfo.map {
    (cInf : ScrayTColumnInfo) => cInf.tType.get.className.getOrElse(cInf.tType.get.tType.name)
  }.toOption

  private def getThriftColInfo()(implicit query : SQuery) : Try[ScrayTColumnInfo] = Try(query.tQuery.queryInfo.columns
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
trait SPostPredicate extends SQueryComponent

case class SOrdering(name : String) extends SPostPredicate {
  def generate()(implicit query : SQuery) : ColumnOrdering[_] = ColumnOrdering(query.getColumn(name).get.generate)(ordered)
}

case class SGrouping(name : String) extends SPostPredicate {
  def generate()(implicit query : SQuery) : ColumnGrouping = ColumnGrouping(query.getColumn(name).get.generate)
}

case class SRange(skip : Option[String], limit : Option[String]) extends SQueryComponent {
  def generate() = QueryRange(skip = skip.map { _.toLong }, limit = limit.map { _.toLong })
}

/**
 * Table type
 *
 * Tables can be specified by means of a literal or thrift reference.
 *
 */
trait STable extends SQueryComponent

case class SSpecTable(dbSystem : String, dbId : String, tabId : String) extends STable {
  def generate() : TableIdentifier = TableIdentifier(dbSystem, dbId, tabId)
}

case class SRefTable(reference : String) extends STable {
  def generate()(implicit query : SQuery) : Try[TableIdentifier] = Try(asSpecTable.get.generate)
  def asSpecTable()(implicit query : SQuery) : Try[SSpecTable] = {
    val ttab = query.tQuery.queryInfo.tableInfo
    if (ttab.tableId.equals(reference)) {
      Success(SSpecTable(ttab.dbSystem, ttab.dbId, ttab.tableId))
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
trait SPredicate extends SQueryComponent {

  /**
   * Explicit column matching
   * @param columns set of columns to check against
   * @return the predicate if success, exception otherwise
   */
  def getMatched(columns : SColumns) : Try[SPredicate]

  /**
   * Generates equivalent query clause
   * @param query query context
   * @return the query clause
   */
  def generate()(implicit query : SQuery) : Clause
}

/**
 * Predicates containing subpredicates
 */
abstract class SComplexPredicate(subpredicates : List[SPredicate]) extends SPredicate

/**
 * AND predicate
 */
case class SAnd(subpredicates : List[SPredicate]) extends SComplexPredicate(subpredicates) {
  override def generate()(implicit query : SQuery) = And(subpredicates.map(_.generate) : _*)
  override def getMatched(columns : SColumns) = Try { SAnd(subpredicates.map { _.getMatched(columns).get }) }
}

/**
 * OR predicate
 */
case class SOr(subpredicates : List[SPredicate]) extends SComplexPredicate(subpredicates) {
  override def generate()(implicit query : SQuery) = { Or(subpredicates.map(_.generate) : _*) }
  override def getMatched(columns : SColumns) = Try { SOr(subpredicates.map { _.getMatched(columns).get }) }
}

/**
 * Atomic predicate type
 */
abstract class SAtomicPredicate(columnName : String, value : SValue) extends SPredicate {

  override def getMatched(columns : SColumns) : Try[SPredicate] = if (isMatchedBy(columns)) Success(this) else
    Failure(new ScrayServiceException(
      ExceptionIDs.PARSING_ERROR,
      query = None,
      s"Predicate contains unmatched column name '${columnName}'.",
      cause = None))

  def isMatchedBy(columns : SColumns) : Boolean = columns match {
    case ac : SAsterixColumn => true
    case cs : SColumnSet => cs.components.find(_.getName.equals(columnName)).nonEmpty
  }

  def getColumn()(implicit query : SQuery) : Column = query.getColumn(columnName).get.generate
  def getValue()(implicit query : SQuery) : Try[Comparable[_]] = Try {
    val aval = value match {
      case lv : SLitVal => lv.deserializeLiteral.get
      case rv : SRefVal => rv.deserializeValue()(query.tQuery).get
    }
    aval.asInstanceOf[Comparable[_]]
  }
}

/*
 * Concrete atomic predicates (=,<,>,<=,>=) containing individual generators
 */

case class SEqual(columnName : String, value : SValue) extends SAtomicPredicate(columnName, value) {
  def generate()(implicit query : SQuery) : Equal[Ordered[_]] = Equal[Ordered[_]](getColumn, getValue.get)
}
case class SGreater(columnName : String, value : SValue) extends SAtomicPredicate(columnName, value) {
  def generate()(implicit query : SQuery) : Greater[Ordered[_]] = Greater[Ordered[_]](getColumn, getValue.get)
}
case class SGreaterEqual(columnName : String, value : SValue) extends SAtomicPredicate(columnName, value) {
  def generate()(implicit query : SQuery) : GreaterEqual[Ordered[_]] = GreaterEqual[Ordered[_]](getColumn, getValue.get)
}
case class SSmaller(columnName : String, value : SValue) extends SAtomicPredicate(columnName, value) {
  def generate()(implicit query : SQuery) : Smaller[Ordered[_]] = Smaller[Ordered[_]](getColumn, getValue.get)
}
case class SSmallerEqual(columnName : String, value : SValue) extends SAtomicPredicate(columnName, value) {
  def generate()(implicit query : SQuery) : SmallerEqual[Ordered[_]] = SmallerEqual[Ordered[_]](getColumn, getValue.get)
}

/**
 * Value types
 */
trait SValue

/**
 * Adapter for literal parser
 */
case class SLitVal(literal : String, typeTag : Option[String]) extends SValue {
  def deserializeLiteral() : Try[_] = typeTag match {
    case Some(typeTag) => StringLiteralDeserializer.deserialize(literal, typeTag)
    case None => StringLiteralDeserializer.deserialize(literal)
  }
  def typeCheck(typName : String) : Option[Boolean] = Option(typeTag.getOrElse(None).equals(typName))
}

/**
 * Adapter for deserializer framework
 */
case class SRefVal(ref : String) extends SValue {
  def deserializeValue()(implicit tQuery : ScrayTQuery) : Try[Any] = throw new NotImplementedError
  def typeCheck()(implicit tQuery : ScrayTQuery) : Option[Boolean] = throw new NotImplementedError
}
