package scray.core.service

import shapeless._
import java.text.SimpleDateFormat
import java.util.UUID
import org.parboiled2._
import com.twitter.util._
import scala.util.{ Try => STry }
import scray.common.exceptions._
import scray.querying.Query
import scray.querying.description._
import scray.querying.queries.SimpleQuery
import scray.service.base.thrifscala.STypeInfo
import scray.service.qmodel.thrifscala.SColumnInfo
import scray.service.qmodel.thrifscala.SQuery
import scray.service.qmodel.thrifscala.SQueryInfo
import java.util.GregorianCalendar
import scray.common.serialization.StringLiteralDeserializer
import scala.util.Failure
import scala.util.Success

object Ordnung {
  import scala.math.Ordering._
  implicit def ordered[A <: Ordered[_]] : Ordering[A] = new Ordering[A] {
    override def compare(x : A, y : A) : Int = x.asInstanceOf[Ordered[A]] compareTo y
  }
}

class QueryParser(tQuery : SQuery) extends Parser {
  override val input : ParserInput = tQuery.queryExpression

  implicit def wspStr(s : String) : Rule0 = rule { str(s) ~ zeroOrMore(' ') }

  def InputLine : Rule1[STry[Query]] = rule { _queryBuilder ~ EOI }

  // Rule for constructing simple queries from parsing results
  def _queryBuilder : Rule1[STry[Query]] = rule { _queryStatement ~> { (q : STry[_Query]) => q.get createQuery } }

  // main query structure
  def _queryStatement : Rule1[STry[_Query]] = rule { _orderbyStatement }
  def _orderbyStatement : Rule1[STry[_Query]] = rule { _groupbyStatement ~ optional(_orderby) ~> { (q : STry[_Query], o : Option[_Ordering]) => q.get addOrdering o } }
  def _groupbyStatement : Rule1[STry[_Query]] = rule { _predicateStatement ~ optional(_groupby) ~> { (q : STry[_Query], g : Option[_Grouping]) => q.get addGrouping g } }
  def _predicateStatement : Rule1[STry[_Query]] = rule { _tableStatement ~ optional("WHERE" ~ _predicate) ~> { (q : STry[_Query], p : Option[_Predicate]) => q.get addPredicate (p) } }
  def _tableStatement : Rule1[STry[_Query]] = rule { _columnStatement ~ "FROM" ~ _table ~> { (q : STry[_Query], t : _Table) => q.get addTable t } }
  def _columnStatement : Rule1[STry[_Query]] = rule { _rootStatement ~ "SELECT" ~ _columns ~> { (q : _Query, c : _Columns) => q addColumns c } }
  def _rootStatement : Rule1[_Query] = rule { push(_Query(List[_QueryComponent](), tQuery)) }

  // Rules matching 'post predicates'
  def _groupby : Rule1[_Grouping] = rule { "GROUPBY" ~ _identifier ~> { (nam : String) => _Grouping(nam) } }
  def _orderby : Rule1[_Ordering] = rule { "ORDERBY" ~ _identifier ~> { (nam : String) => _Ordering(nam) } }

  // Rules matching columns
  def _columns : Rule1[_Columns] = rule { _asterixColumn | _columnSet }
  def _asterixColumn : Rule1[_AsterixColumn] = rule { "*" ~ push(_AsterixColumn()) }
  def _columnSet : Rule1[_ColumnSet] = rule { oneOrMore(_column) ~> { (cols : Seq[_Column]) => _ColumnSet(cols.toList) } }
  def _column : Rule1[_Column] = rule { _refColum | _specColumn }
  def _specColumn : Rule1[_SpecColumn] = rule { _typedColumn | _untypedColumn }
  def _typedColumn : Rule1[_SpecColumn] = rule { _typetag ~ oneOrMore(' ') ~ _identifier ~> { (tag : String, id : String) => _SpecColumn(id, Some(tag)) } }
  def _untypedColumn : Rule1[_SpecColumn] = rule { _identifier ~> { (id : String) => _SpecColumn(id, None) } }
  def _refColum : Rule1[_RefColumn] = rule { _reference ~> { (ref : String) => _RefColumn(ref) } }

  // Rules matching the table part - this might throw
  def _table : Rule1[_Table] = rule { (_tableRef | _tableSpec) }
  def _tableSpec : Rule1[_SpecTable] = rule { _tableSyntax ~> { (a : String, b : String, c : String) => _SpecTable(a, b, c) } }
  def _tableSyntax = rule { "(" ~ "dbSystem" ~ "=" ~ _identifier ~ "," ~ "dbId" ~ "=" ~ _identifier ~ "," ~ "tabId" ~ "=" ~ _identifier ~ ")" }
  def _tableRef : Rule1[_RefTable] = rule { _reference ~> { (ref : String) => _RefTable(ref) } }

  // Rules matching complex predicates
  def _predicate : Rule1[_Predicate] = rule { "(" ~ _predicate ~ ")" | _and | _or | _atomicPredicate }
  def _and : Rule1[_ComplexPredicate] = rule { (_predicate ~ oneOrMore("AND" ~ _predicate)) ~> { (pre1 : _Predicate, pre2 : Seq[_Predicate]) => _And(pre1 :: pre2.toList) } }
  def _or : Rule1[_ComplexPredicate] = rule { (_predicate ~ oneOrMore("OR" ~ _predicate)) ~> { (pre1 : _Predicate, pre2 : Seq[_Predicate]) => _Or(pre1 :: pre2.toList) } }

  // Rules matching atomic predicates
  def _atomicPredicate : Rule1[_AtomicPredicate] = rule { _equal | _greater | _smaller | _greaterEqual | _smallerEqual }
  def _equal : Rule1[_Equal] = rule { (_identifier ~ "=" ~ _value) ~> { _Equal(_, _) } }
  def _greater : Rule1[_Greater] = rule { (_identifier ~ ">" ~ _value) ~> { _Greater(_, _) } }
  def _smaller : Rule1[_Smaller] = rule { (_identifier ~ "<" ~ _value) ~> { _Smaller(_, _) } }
  def _greaterEqual : Rule1[_GreaterEqual] = rule { (_identifier ~ ">=" ~ _value) ~> { _GreaterEqual(_, _) } }
  def _smallerEqual : Rule1[_SmallerEqual] = rule { (_identifier ~ "<=" ~ _value) ~> { _SmallerEqual(_, _) } }

  // rules matching values - these might throw
  def _value : Rule1[_Value] = rule { _typedLiteralValue | _referencedValue | _plainLiteralValue }
  def _typedLiteralValue : Rule1[_LitVal] = rule { _typedLiteral ~> { (lit : String, tag : String) => _LitVal(lit, Some(tag)) } }
  def _plainLiteralValue : Rule1[_LitVal] = rule { _untypedLiteral ~> { (lit : String) => _LitVal(lit, None) } }
  def _referencedValue : Rule1[_RefVal] = rule { _reference ~> { (ref : String) => _RefVal(ref) } }

  // Rules matching value literals
  def _typedLiteral : Rule2[String, String] = rule { _typetag ~ oneOrMore(' ') ~ _charSet }
  def _untypedLiteral : Rule1[String] = rule { _charSet }

  // Rules matching terminals
  def _typetag : Rule1[String] = rule { capture(str("!!") ~ oneOrMore(CharPredicate.AlphaNum) ~ zeroOrMore(' ')) } // tag w/ '!!'
  def _reference : Rule1[String] = rule { str("@") ~ capture(_token) } // ref w/o '@'
  def _identifier : Rule1[String] = rule { capture(oneOrMore(CharPredicate.AlphaNum)) ~ zeroOrMore(' ') }
  def _token : Rule0 = rule { oneOrMore(CharPredicate.AlphaNum) ~ zeroOrMore(' ') }
  def _charSet : Rule1[String] = rule { capture(oneOrMore(CharPredicate.Visible)) ~ zeroOrMore(' ') }
}

/*
 * query builder model
 */

trait _QueryComponent

case class _Query(components : List[_QueryComponent], tQuery : SQuery) {
  // query generator
  def createQuery() : STry[Query] = ???

  // consecutive builder functions
  def addColumns(columns : _Columns) : STry[_Query] = ???
  def addTable(table : _Table) : STry[_Query] = ???
  def addPredicate(predicate : Option[_Predicate]) : STry[_Query] = ???
  def addGrouping(grouping : Option[_Grouping]) : STry[_Query] = ???
  def addOrdering(ordering : Option[_Ordering]) : STry[_Query] = ???

  // helpers
  def getTabIdFromRef() = TableIdentifier(
    dbSystem = tQuery.queryInfo.tableInfo.dbSystem,
    dbId = tQuery.queryInfo.tableInfo.dbId,
    tableId = tQuery.queryInfo.tableInfo.tableId)
  def hasColumn(name : String) : Boolean = getColumn(name).isDefined
  def getColumn(name : String) : Option[_Column] =
    components.find(_ match {
      case col : _Column if (name.equals(col.getName)) => true
      case _ => false
    }).asInstanceOf[Option[_Column]]
}

trait _Columns extends _QueryComponent
case class _ColumnSet(components : List[_Column]) extends _Columns
case class _AsterixColumn extends _Columns

/**
 * Column type responsible for implicit parameter validation and explicit ref matching
 */
trait _Column extends _QueryComponent {
  def getName : String
  def get(table : _SpecTable) : STry[Column]
}
case class _SpecColumn(name : String, typeTag : Option[String]) extends _Column {
  override def getName = name

  // add column by value (doesn't require to match thrift column list)
  override def get(table : _SpecTable) : STry[Column] =
    if ((!getName.isEmpty())) {
      STry(Column(getName, table.get.get))
    } else {
      Failure(new ScrayServiceException(ExceptionIDs.PARSING_ERROR, query = None, "Malformed column literal.", cause = None))
    }
}
case class _RefColumn(reference : String) extends _Column {
  override def getName = reference
  override def get(table : _SpecTable) : STry[Column] = asSpecColumn.get(table)

  // cast to unmatched spec column
  def asSpecColumn() = _SpecColumn(reference, None)

  // returns 1. class name or 2. type tag or 3. None
  def getType(tQuery : SQuery) : Option[String] =
    find(tQuery).map { (cInf : SColumnInfo) => cInf.sType.get.className.getOrElse(cInf.sType.get.sType.name) }.toOption

  def find(tQuery : SQuery) : STry[SColumnInfo] =
    STry(tQuery.queryInfo.columns
      .find(col => col.name.equals(reference))
      .getOrElse(throw new ScrayServiceException(ExceptionIDs.PARSING_ERROR, query = None, s"Unmatched column reference '$getName'.", cause = None)))

  def matchedGet(table : _SpecTable, tQuery : SQuery) : STry[Column] = find(tQuery).map { (cInf : SColumnInfo) => Column(cInf.name, table.get.get) }
}

/**
 * PostPredicate (AND, OR) type responsible for explicit column matching
 */
class _PostPredicate(name : String) extends _QueryComponent {
  def isRefMatched(tQuery : SQuery) : Boolean = _RefColumn(name).find(tQuery).isSuccess
  def isQueryMatched(_query : _Query) : Boolean = _query.components.find {
    _ match {
      case col : _Column if (col.getName.equals(name)) => true
      case _ => false
    }
  }.isDefined
}
case class _Ordering(name : String) extends _PostPredicate(name) {
  def get(column : _Column, table : _SpecTable) : STry[ColumnOrdering[_]] = STry(ColumnOrdering(column.get(table).get)(Ordnung.ordered))
}
case class _Grouping(name : String) extends _PostPredicate(name) {
  def get(column : _Column, table : _SpecTable) : STry[ColumnGrouping] = STry(ColumnGrouping(column.get(table).get))
}

/**
 * Table type responsible for implicit parameter validation
 */
trait _Table extends _QueryComponent
case class _SpecTable(dbSystem : String, dbId : String, tabId : String) extends _Table {
  def get() : STry[TableIdentifier] = {
    if ((!dbSystem.isEmpty()) && (!dbId.isEmpty) && (!tabId.isEmpty)) {
      Success(TableIdentifier(dbSystem, dbId, tabId))
    } else {
      Failure(new ScrayServiceException(ExceptionIDs.PARSING_ERROR, query = None, "Malformed table literal.", cause = None))
    }
  }
}
case class _RefTable(reference : String) extends _Table {
  def get(tQuery : SQuery) : STry[TableIdentifier] = STry(asSpecTable(tQuery).get.get.get)
  def asSpecTable(tQuery : SQuery) : STry[_SpecTable] = {
    val ttab = tQuery.queryInfo.tableInfo
    if (ttab.tableId.equals(reference)) {
      Success(_SpecTable(ttab.dbSystem, ttab.dbId, ttab.tableId))
    } else {
      Failure(new ScrayServiceException(ExceptionIDs.PARSING_ERROR, query = None, s"Unmatched table reference '$reference'.", cause = None))
    }
  }
}

/**
 * Predicate type
 */
trait _Predicate extends _QueryComponent
class _ComplexPredicate(subpredicates : List[_Predicate]) extends _Predicate
case class _And(subpredicates : List[_Predicate]) extends _ComplexPredicate(subpredicates)
case class _Or(subpredicates : List[_Predicate]) extends _ComplexPredicate(subpredicates)

/**
 * AtomicPredicate type TODO: responsible for explicit column matching
 */
class _AtomicPredicate(columnName : String, value : _Value) extends _Predicate
case class _Equal(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value)
case class _Greater(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value)
case class _Smaller(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value)
case class _GreaterEqual(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value)
case class _SmallerEqual(columnName : String, value : _Value) extends _AtomicPredicate(columnName, value)

/**
 * Value type responsible for imlicit deserialization TODO: and explicit ref matching
 */
trait _Value
case class _LitVal(literal : String, typeTag : Option[String]) extends _Value {
  // adapter function for literal parser
  def deserializeLiteral(literal : String, ttag : Option[String]) : STry[Any] = ttag match {
    case Some(tag) => StringLiteralDeserializer.deserialize(literal, tag)
    case None => StringLiteralDeserializer.deserialize(literal)
  }
  def typeCheck(typName : String) : Option[Boolean] = Option(typeTag.getOrElse(None).equals(typName))
}
case class _RefVal(ref : String) extends _Value {
  // TODO: adapter function for thrift binary deserializer
  def deserializeValue(valueRef : String, tQuery : SQuery) : STry[Any] = ???
  def typeCheck(tQuery : SQuery) : Option[Boolean] = ???
}
