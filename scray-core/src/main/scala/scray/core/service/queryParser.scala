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
import scray.querying.description.And
import scray.querying.description.Clause
import scray.querying.description.Column
import scray.querying.description.ColumnGrouping
import scray.querying.description.ColumnOrdering
import scray.querying.description.Columns
import scray.querying.description.Equal
import scray.querying.description.Greater
import scray.querying.description.GreaterEqual
import scray.querying.description.Or
import scray.querying.description.Smaller
import scray.querying.description.SmallerEqual
import scray.querying.description.TableIdentifier
import scray.querying.description._
import scray.querying.queries.SimpleQuery
import scray.service.qmodel.thrifscala.SColumnInfo
import scray.service.qmodel.thrifscala.SQuery
import Ordnung.ordered

object Ordnung {
  import scala.math.Ordering._
  implicit def ordered[A <: Comparable[_]] : Ordering[A] = new Ordering[A] {
    override def compare(x : A, y : A) : Int = x.asInstanceOf[Comparable[A]] compareTo y
  }
}

class QueryParser(tQuery : SQuery) extends Parser {
  override val input : ParserInput = tQuery.queryExpression

  implicit def wspStr(s : String) : Rule0 = rule { str(s) ~ zeroOrMore(' ') }

  def InputLine : Rule1[Try[Query]] = rule { _queryBuilder ~ EOI }

  // Rule for constructing simple queries from parsing results
  def _queryBuilder : Rule1[Try[Query]] = rule { _queryStatement ~> { (q : Try[_Query]) => q.get createQuery } }

  // main query structure
  def _queryStatement : Rule1[Try[_Query]] = rule { _rangeStatement }
  def _rangeStatement : Rule1[Try[_Query]] = rule { _orderbyStatement ~ optional(_range) ~> { (q : Try[_Query], o : Option[_Range]) => q.get addRange o } }
  def _orderbyStatement : Rule1[Try[_Query]] = rule { _groupbyStatement ~ optional(_orderby) ~> { (q : Try[_Query], o : Option[_Ordering]) => q.get addOrdering o } }
  def _groupbyStatement : Rule1[Try[_Query]] = rule { _predicateStatement ~ optional(_groupby) ~> { (q : Try[_Query], g : Option[_Grouping]) => q.get addGrouping g } }
  def _predicateStatement : Rule1[Try[_Query]] = rule { _tableStatement ~ optional("WHERE" ~ _predicate) ~> { (q : Try[_Query], p : Option[_Predicate]) => q.get addPredicate (p) } }
  def _tableStatement : Rule1[Try[_Query]] = rule { _columnStatement ~ "FROM" ~ _table ~> { (q : Try[_Query], t : _Table) => q.get addTable t } }
  def _columnStatement : Rule1[Try[_Query]] = rule { _rootStatement ~ "SELECT" ~ _columns ~> { (q : _Query, c : _Columns) => q addColumns c } }
  def _rootStatement : Rule1[_Query] = rule { push(_Query(Map[Class[_ <: _QueryComponent], _QueryComponent]())(tQuery)) }

  // Rules matching ranges
  def _range : Rule1[_Range] = rule { (_limit ~ push(None) | push(None) ~ _skip | _limit ~ _skip) ~> { (l : Option[String], s : Option[String]) => _Range(s, l) } }
  def _limit : Rule1[Option[String]] = rule { "LIMIT" ~ _number ~> { (s : String) => Some(s) } }
  def _skip : Rule1[Option[String]] = rule { "SKIP" ~ _number ~> { (s : String) => Some(s) } }

  // Rules matching 'post predicates'
  def _groupby : Rule1[_Grouping] = rule { "GROUPBY" ~ _identifier ~> { (nam : String) => _Grouping(nam) } }
  def _orderby : Rule1[_Ordering] = rule { "ORDERBY" ~ _identifier ~> { (nam : String) => _Ordering(nam) } }

  // Rules matching columns
  def _columns : Rule1[_Columns] = rule { _asterixColumn | _columnSet }
  def _asterixColumn : Rule1[_AsterixColumn] = rule { "*" ~ push(_AsterixColumn()) }
  def _columnSet : Rule1[_ColumnSet] = rule { _column ~ zeroOrMore("," ~ _column) ~> { (col : _Column, cols : Seq[_Column]) => _ColumnSet(col :: cols.toList) } }
  def _column : Rule1[_Column] = rule { _refColum | _specColumn }
  def _specColumn : Rule1[_SpecColumn] = rule { _typedColumn | _untypedColumn }
  def _typedColumn : Rule1[_SpecColumn] = rule { _typetag ~ _identifier ~> { (tag : String, id : String) => _SpecColumn(id, Some(tag)) } }
  def _untypedColumn : Rule1[_SpecColumn] = rule { _identifier ~> { (id : String) => _SpecColumn(id, None) } }
  def _refColum : Rule1[_RefColumn] = rule { _reference ~> { (ref : String) => _RefColumn(ref) } }

  // Rules matching the table part - this might throw
  def _table : Rule1[_Table] = rule { (_tableRef | _tableSpec) }
  def _tableSpec : Rule1[_SpecTable] = rule { _tableSyntax ~> { (a : String, b : String, c : String) => _SpecTable(a, b, c) } }
  def _tableSyntax = rule { "(" ~ "dbSystem" ~ "=" ~ _identifier ~ "," ~ "dbId" ~ "=" ~ _identifier ~ "," ~ "tabId" ~ "=" ~ _identifier ~ ")" }
  def _tableRef : Rule1[_RefTable] = rule { _reference ~> { (ref : String) => _RefTable(ref) } }

  // Rules matching complex predicates in disjunctive cannonical form
  def _predicate : Rule1[_Predicate] = rule { _disjunction | _conjunction | _factor }
  def _disjunction : Rule1[_Predicate] = rule { _term ~ oneOrMore("OR" ~ _term) ~> { (pre1 : _Predicate, pre2 : Seq[_Predicate]) => _Or(pre1 :: pre2.toList) } }
  def _term : Rule1[_Predicate] = rule { _conjunction | _factor }
  def _conjunction : Rule1[_Predicate] = rule { _factor ~ oneOrMore("AND" ~ _factor) ~> { (pre1 : _Predicate, pre2 : Seq[_Predicate]) => _And(pre1 :: pre2.toList) } }
  def _factor : Rule1[_Predicate] = rule { _parens | _atomicPredicate }
  def _parens : Rule1[_Predicate] = rule { "(" ~ (_conjunction | _disjunction | _factor) ~ ")" }

  // Rules matching atomic predicates
  def _atomicPredicate : Rule1[_AtomicPredicate] = rule { _equal | _greaterEqual | _smallerEqual | _greater | _smaller }
  def _equal : Rule1[_Equal] = rule { _identifier ~ "=" ~ _value ~> { _Equal(_, _) } }
  def _greater : Rule1[_Greater] = rule { _identifier ~ ">" ~ _value ~> { _Greater(_, _) } }
  def _smaller : Rule1[_Smaller] = rule { _identifier ~ "<" ~ _value ~> { _Smaller(_, _) } }
  def _greaterEqual : Rule1[_GreaterEqual] = rule { _identifier ~ ">=" ~ _value ~> { _GreaterEqual(_, _) } }
  def _smallerEqual : Rule1[_SmallerEqual] = rule { _identifier ~ "<=" ~ _value ~> { _SmallerEqual(_, _) } }

  // rules matching values - these might throw
  def _value : Rule1[_Value] = rule { _typedLiteralValue | _referencedValue | _plainLiteralValue }
  def _typedLiteralValue : Rule1[_LitVal] = rule { _typedLiteral ~> { (tag : String, lit : String) => _LitVal(lit, Some(tag)) } }
  def _plainLiteralValue : Rule1[_LitVal] = rule { _untypedLiteral ~> { (lit : String) => _LitVal(lit, None) } }
  def _referencedValue : Rule1[_RefVal] = rule { _reference ~> { (ref : String) => _RefVal(ref) } }

  // Rules matching value literals
  def _typedLiteral : Rule2[String, String] = rule { _typetag ~ _charSet }
  def _untypedLiteral : Rule1[String] = rule { _charSet }

  // Rules matching terminals
  def _typetag : Rule1[String] = rule { capture(str("!!") ~ oneOrMore(CharPredicate.AlphaNum)) ~ oneOrMore(' ') } // tag w/ '!!'
  def _reference : Rule1[String] = rule { str("@") ~ _identifier } // ref w/o '@'
  def _identifier : Rule1[String] = rule { capture(oneOrMore(CharPredicate.AlphaNum)) ~ zeroOrMore(' ') }
  def _charSet : Rule1[String] = rule { capture(oneOrMore(CharPredicate.Visible)) ~ zeroOrMore(' ') }
  def _number : Rule1[String] = rule { capture(oneOrMore(CharPredicate.Digit)) ~ zeroOrMore(' ') }
}

/*
 * query builder model
 */

trait _QueryComponent

case class _Query(components : Map[Class[_ <: _QueryComponent], _QueryComponent])(implicit val tQuery : SQuery) {

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
  def generate()(implicit _query : _Query) : Columns
}
case class _ColumnSet(components : List[_Column]) extends _Columns {
  override def isContained(cName : String) : Boolean = find(cName).nonEmpty
  override def find(cName : String) : Option[_Column] = components.find(_.getName.equals(cName))
  override def generate()(implicit _query : _Query) = Columns(Right(components.map { _.generate } toList))
}
case class _AsterixColumn extends _Columns {
  override def isContained(cName : String) : Boolean = true
  override def find(cName : String) : Option[_Column] = Some(_SpecColumn(cName, None))
  override def generate()(implicit _query : _Query) = Columns(Left(true))
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
    (cInf : SColumnInfo) => cInf.sType.get.className.getOrElse(cInf.sType.get.sType.name)
  }.toOption

  private def getThriftColInfo()(implicit query : _Query) : Try[SColumnInfo] = Try(query.tQuery.queryInfo.columns
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
  def generate()(implicit query : _Query) : ColumnOrdering[_] = ColumnOrdering(query.getColumn(name).get.generate)(Ordnung.ordered)
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
  def deserializeValue()(implicit tQuery : SQuery) : Try[Any] = throw new NotImplementedError
  def typeCheck()(implicit tQuery : SQuery) : Option[Boolean] = throw new NotImplementedError
}
