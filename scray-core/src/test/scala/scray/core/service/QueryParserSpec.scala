package scray.core.service

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.util.Success
import scala.util.Failure
import org.parboiled2.ParseError
import scray.service.qmodel.thrifscala.SQuery
import java.nio.ByteBuffer

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
      columns = Set(scray.service.qmodel.thrifscala.SColumnInfo(
        name = "col1",
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

  val expressions = Array(
    "SELECT * FROM @myTableId",
    "SELECT col1 FROM @myTableId",
    "SELECT col1, col2 FROM @myTableId",
    "SELECT col1 FROM @myTableId WHERE col1 > val1 AND col1 < val2 OR col1 = val3",
    "SELECT col1, col2 FROM @myTableId ORDERBY col1",
    "SELECT col1, col2 FROM @myTableId GROUPBY col1")

  "query parser" should "parse default queries" in {
    expressions.map(expr => {
      print(s"Parsing '$expr' ... ")
      val parser = new QueryParser2(createSQuery(expr))
      val parsed = parser.InputLine.run() match {
        case Success(result) => Success(result)
        case Failure(e : ParseError) =>
          sys.error(parser.formatError(e, showTraces = true)); Failure(e)
        case Failure(e) => throw e
      }
      parsed.isSuccess should be(true)
      println(s"success")
    })
  }

}
