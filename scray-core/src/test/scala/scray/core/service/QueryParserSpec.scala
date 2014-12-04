package scray.core.service

import java.nio.ByteBuffer
import scala.Option.option2Iterable
import scala.util.Failure
import scala.util.Success
import org.junit.runner.RunWith
import org.parboiled2.ParseError
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scray.common.exceptions.ScrayServiceException
import scray.querying.description.And
import scray.querying.description.Columns
import scray.querying.description.Equal
import scray.querying.description.Greater
import scray.querying.description.GreaterEqual
import scray.querying.description.Smaller
import scray.querying.description.SmallerEqual
import scray.service.qmodel.thrifscala.SQuery
import org.scalatest.junit.JUnitRunner
import scala.util.Try
import scray.querying.description.Or

@RunWith(classOf[JUnitRunner])
class QueryParserSpec extends FlatSpec with Matchers {

  def createSQuery(expr : String) : SQuery = SQuery(
    queryInfo = scray.service.qmodel.thrifscala.SQueryInfo(
      queryId = Some(scray.service.base.thrifscala.Uuid(1, 2)),
      querySpace = "myQuerySpace",
      tableInfo = scray.service.qmodel.thrifscala.STableInfo(
        dbSystem = "myDbSystem",
        dbId = "myDbId",
        tableId = "myTableId",
        keyT = scray.service.base.thrifscala.STypeInfo(
          scray.service.base.thrifscala.SType(1),
          Some("scray.classname"))),
      columns = Set(
        scray.service.qmodel.thrifscala.SColumnInfo(
          name = "col1",
          None /* STypeInfo */ ,
          None /* STableInfo */ ),
        scray.service.qmodel.thrifscala.SColumnInfo(
          name = "col2",
          None /* STypeInfo */ ,
          None /* STableInfo */ ))),
    values = Map(
      "val1" -> scray.service.base.thrifscala.SValue(
        scray.service.base.thrifscala.STypeInfo(
          scray.service.base.thrifscala.SType(2),
          Some("scray.classname")),
        ByteBuffer.allocate(10)),
      "val2" -> scray.service.base.thrifscala.SValue(
        scray.service.base.thrifscala.STypeInfo(
          scray.service.base.thrifscala.SType(2),
          Some("scray.classname")),
        ByteBuffer.allocate(10)),
      "val3" -> scray.service.base.thrifscala.SValue(
        scray.service.base.thrifscala.STypeInfo(
          scray.service.base.thrifscala.SType(2),
          Some("scray.classname")),
        ByteBuffer.allocate(10))),
    queryExpression = expr)

  val DEBUG = true

  private def parse(query : String) = {
    val parser = new QueryParser(createSQuery(query))
    val parsed = parser.InputLine.run() match {
      case Success(result) => Success(result)
      case Failure(e : ParseError) =>
        sys.error(parser.formatError(e, showTraces = true)); Failure(e)
      case Failure(e) => throw e
    }
    if (DEBUG) println(s"Parsed '$query' ... $parsed.get")
    parsed
  }

  private def checkGood(result : Product with Serializable with Try[Try[scray.querying.Query]]) : Unit = {
    result.isSuccess should be(true)
    result.get.isSuccess should be(true)
  }

  "query parser" should "handle asterix columns" in {
    val parsed = parse("SELECT * FROM @myTableId")
    checkGood(parsed)
    parsed.get.get.getResultSetColumns should be(Columns(Left(true)))
  }

  it should "handle single column refs" in {
    val parsed = parse("SELECT @col1 FROM @myTableId")
    checkGood(parsed)
    parsed.get.get.getResultSetColumns.columns.right.get.size should be(1)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("col1")).size should be(1)
  }

  val query3 = "SELECT * FROM @myTableId"

  it should "handle multi column refs" in {
    val parsed = parse("SELECT @col1, @col2 FROM @myTableId")
    checkGood(parsed)
    parsed.get.get.getResultSetColumns.columns.right.get.size should be(2)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("col1")).size should be(1)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("col2")).size should be(1)
  }

  it should "throw with unmatched column refs" in {
    an[ScrayServiceException] should be thrownBy parse("SELECT @col1, @col2, @col3 FROM @myTableId")
  }

  it should "handle single column literals" in {
    val parsed = parse("SELECT foo FROM @myTableId")
    checkGood(parsed)
    parsed.get.get.getResultSetColumns.columns.right.get.size should be(1)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("foo")).size should be(1)
  }

  it should "handle multi column literals" in {
    val parsed = parse("SELECT foo, bar FROM @myTableId")
    checkGood(parsed)
    parsed.get.get.getResultSetColumns.columns.right.get.size should be(2)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("foo")).size should be(1)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("bar")).size should be(1)
  }

  it should "handle single typed column literals" in {
    val parsed = parse("SELECT !!long foo FROM @myTableId")
    checkGood(parsed)
    // no test for type meta info at this level
    parsed.get.get.getResultSetColumns.columns.right.get.size should be(1)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("foo")).size should be(1)
  }

  it should "handle multiple typed column literals" in {
    val parsed = parse("SELECT !!long foo, !!string bar FROM @myTableId")
    checkGood(parsed)
    // no test for type meta info at this level
    parsed.get.get.getResultSetColumns.columns.right.get.size should be(2)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("foo")).size should be(1)
    parsed.get.get.getResultSetColumns.columns.right.get.find(_.columnName.equals("bar")).size should be(1)
  }

  it should "handle table literals" in {
    val parsed = parse("SELECT * FROM (dbSystem=foo, dbId=bar, tabId=baz)")
    checkGood(parsed)
    parsed.get.get.getTableIdentifier.dbId should be("bar")
    parsed.get.get.getTableIdentifier.dbSystem should be("foo")
    parsed.get.get.getTableIdentifier.tableId should be("baz")
  }

  it should "handle table references" in {
    val parsed = parse("SELECT * FROM @myTableId")
    checkGood(parsed)
    parsed.get.get.getTableIdentifier.dbId should be("myDbId")
    parsed.get.get.getTableIdentifier.dbSystem should be("myDbSystem")
    parsed.get.get.getTableIdentifier.tableId should be("myTableId")
  }

  it should "handle atomic predicates with literal values" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1=1")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.asInstanceOf[Equal[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with typed literal values" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1 = !!long 2")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.asInstanceOf[Equal[Long]].value.asInstanceOf[Ordered[Long]].compareTo(2L) should be(0)
  }

  it should "handle atomic predicates with 'equal'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1=1")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[Equal[_]] should be(true)
    parsed.get.get.getWhereAST.get.asInstanceOf[Equal[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with 'smaller'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1<1")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[Smaller[_]] should be(true)
    parsed.get.get.getWhereAST.get.asInstanceOf[Smaller[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with 'smaller equal'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1<=1")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[SmallerEqual[_]] should be(true)
    parsed.get.get.getWhereAST.get.asInstanceOf[SmallerEqual[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with 'greater'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1>1")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[Greater[_]] should be(true)
    parsed.get.get.getWhereAST.get.asInstanceOf[Greater[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with 'greater equal'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1>=1")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[GreaterEqual[_]] should be(true)
    parsed.get.get.getWhereAST.get.asInstanceOf[GreaterEqual[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle complex 'AND' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId WHERE col1=1 AND col2=2")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[And] should be(true)
  }

  it should "handle complex 'OR' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId WHERE col1=1 OR col2=2")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[Or] should be(true)
  }

  it should "handle mixed complex predicates as disjunctions" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId WHERE col1>1 OR col2>2 AND col1<10")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[Or] should be(true)
  }

  it should "handle nested complex predicates with parens" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId WHERE ( col1>1 OR col2>2 ) AND col1<10")
    checkGood(parsed)
    parsed.get.get.getWhereAST.get.isInstanceOf[And] should be(true)
  }

  it should "handle 'ORDERBY' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId ORDERBY col2")
    checkGood(parsed)
    parsed.get.get.getOrdering.isDefined should be(true)
  }

  it should "handle 'GROUPBY' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId GROUPBY col2")
    checkGood(parsed)
    parsed.get.get.getGrouping.nonEmpty should be(true)
    parsed.get.get.getGrouping.get.column.columnName should be("col2")
  }

  it should "handle 'LIMIT' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId LIMIT 100")
    checkGood(parsed)
    parsed.get.get.getQueryRange.get.limit.nonEmpty should be(true)
  }

  it should "handle 'SKIP' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId SKIP 100")
    checkGood(parsed)
    parsed.get.get.getQueryRange.get.skip.nonEmpty should be(true)
  }

}
