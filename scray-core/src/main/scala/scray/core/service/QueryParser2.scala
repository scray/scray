package scray.core.service

import org.parboiled2._
import scray.querying.Query
import scray.common.exceptions._
import scray.querying.description._
import scray.service.qmodel.thrifscala.SQuery
import scray.querying.queries.SimpleQuery
import java.util.UUID
import com.twitter.util._

object Ordnung2 {
  import scala.math.Ordering._
  implicit def ordered[A <: Ordered[_]] : Ordering[A] = new Ordering[A] {
    override def compare(x : A, y : A) : Int = x.asInstanceOf[Ordered[A]] compareTo y
  }
}

class QueryParser2(tQuery : SQuery) extends Parser {

  override val input : ParserInput = tQuery.queryExpression

  private val deserializer = new QueryValueDeserializer(tQuery)

  val tableId = new TableIdentifier(
    dbSystem = tQuery.queryInfo.tableInfo.dbSystem,
    dbId = tQuery.queryInfo.tableInfo.dbId,
    tableId = tQuery.queryInfo.tableInfo.tableId)

  implicit def wspStr(s : String) : Rule0 = rule { str(s) ~ zeroOrMore(' ') }

  def InputLine : Rule1[Query] = rule { queryBuildeR ~ EOI }

  // Rule for constructing simple queries from parsing results
  def queryBuildeR = rule {
    queryStatementR ~> {
      (cols : Columns, tbl : TableIdentifier, clas : Option[Clause], grp : Option[ColumnGrouping], ord : Option[ColumnOrdering[Ordered[_]]]) =>
        SimpleQuery(space = tQuery.queryInfo.querySpace, table = tbl, columns = cols, where = clas, grouping = grp, ordering = ord)
    }
  }

  // main query structuring rule
  def queryStatementR = rule { "SELECT" ~ columnsR ~ "FROM" ~ tableR ~ optional("WHERE" ~ predicateR) ~ optional(groupbyR) ~ optional(orderbyR) }

  // Rules matching either one or more column identifiers or an asterix resulting in a 'columns' object
  def columnsR : Rule1[Columns] = rule { columnAsterixR | columnSetR }
  def columnSetR : Rule1[Columns] = rule { oneOrMore(columnR) ~> { (cls : Seq[Column]) => Columns(Right(cls toList)) } }
  def columnAsterixR : Rule1[Columns] = rule("*" ~ push(Columns(Left(true))))

  // Rules matching the table part resulting in a table identifier
  def tableR : Rule1[TableIdentifier] = rule { (tableRefR | tableSpecR) ~> { (tapOp : Try[TableIdentifier]) => tapOp.get } }
  // Reference to SQuery.queryInfo.tableInfo - the table id is being checked!
  def tableSpecR = rule { tableSpecSyntaxR ~> { (a : String, b : String, c : String) => Return(new TableIdentifier(a, b, c)) } }
  // Alternative way to specify table params.
  def tableSpecSyntaxR = rule { "(" ~ "dbSystem" ~ "=" ~ identifieR ~ "," ~ "dbId" ~ "=" ~ identifieR ~ "," ~ "tabId" ~ "=" ~ identifieR ~ ")" }

  def tableRefR = rule {
    str("@") ~ identifieR ~> {
      _ match {
        case ref : String if (tableId.tableId.equals(ref)) => Return(tableId)
        case _ => Throw(new ScrayException(ExceptionIDs.GENERAL_FAULT,
          tQuery.queryInfo.queryId match {
            case Some(u) => Some(new UUID(u.mostSigBits, u.leastSigBits))
            case None => None
          },
          "Table mismatch in query string.", None))
      }
    }
  }

  // Rules matching complex predicates resulting in a clause.
  def predicateR : Rule1[Clause] = rule { "(" ~ predicateR ~ ")" | andPredicateR | orPredicateR | atomicPredicateR }
  def andPredicateR = rule { (predicateR ~ oneOrMore("AND" ~ predicateR)) ~> { (cla : Clause, clb : Seq[Clause]) => And(cla :: clb.toList : _*) } }
  def orPredicateR = rule { (predicateR ~ oneOrMore("OR" ~ predicateR)) ~> { (cla : Clause, clb : Seq[Clause]) => Or(cla :: clb.toList : _*) } }

  // Rules matching atomic predicates resulting in an atomic clause.
  def atomicPredicateR : Rule1[AtomicClause] = rule { equalR | greateR | smalleR | greaterEqualR | smalleR | smallerEqualR }
  def equalR = rule { (columnR ~ "=" ~ valueR) ~> { Equal[Ordered[_]](_, _) } }
  def greateR = rule { (columnR ~ ">" ~ valueR) ~> { Greater[Ordered[_]](_, _)(Ordnung2.ordered) } }
  def smalleR = rule { (columnR ~ "<" ~ valueR) ~> { Smaller[Ordered[_]](_, _)(Ordnung2.ordered) } }
  def greaterEqualR = rule { (columnR ~ ">=" ~ valueR) ~> { GreaterEqual[Ordered[_]](_, _)(Ordnung2.ordered) } }
  def smallerEqualR = rule { (columnR ~ "<=" ~ valueR) ~> { SmallerEqual[Ordered[_]](_, _)(Ordnung2.ordered) } }

  // Rules matching ordering and grouping predicates
  def orderbyR = rule { "ORDERBY" ~ identifieR ~> { (colNam : String) => ColumnOrdering[Ordered[_]](Column(colNam, tableId))(Ordnung2.ordered) } }
  def groupbyR = rule { "GROUPBY" ~ identifieR ~> { (colNam : String) => ColumnGrouping(Column(colNam, tableId)) } }

  // Rules matching identifiers and values
  def columnR : Rule1[Column] = rule { identifieR ~> { (x : String) => Column(x, tableId) } }
  // Matching a value identifier 'v' triggers deserialization of respective SQuery.values.get(v)
  def valueR : Rule1[Ordered[_]] = rule { identifieR ~> { (valId : String) => deserializer.deserialize(valId) } }

  // Rules matching identifiers
  def identifieR : Rule1[String] = rule { capture(wordR) }
  def wordR : Rule0 = rule { oneOrMore(CharPredicate.AlphaNum) ~ zeroOrMore(' ') }
}

case class QueryValueDeserializer(val tQuery : SQuery) {
  def deserialize[_](valueId : String) : Ordered[_] = 1L
}
