package scray.common.serialization

import org.scalatest._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.util._
import org.parboiled2.ParseError
import java.util.UUID
import java.util.Date
import org.parboiled2.ErrorFormatter

@RunWith(classOf[JUnitRunner])
class StringLiteralDeserializerSpec extends FlatSpec with Matchers {

  def parse(literal : String) : Try[Any] = {
    val parser = new StringLiteralDeserializer.LiteralParser(literal)
    parser.InputLine.run() match {
      case Success(result) => Success(result)
      case Failure(e : ParseError) =>
        sys.error(parser.formatError(e, new ErrorFormatter(showTraces = true))); Failure(e)
      case Failure(e) => throw e
    }
  }

  "A LiteralParser" should "parse strings" in {
    parse("'Hello'") should be(Success("Hello"))
  }
  it should "parse integers" in {
    parse("123") should be(Success(123))
  }
  it should "parse floats" in {
    parse("1.5") should be(Success(1.5))
  }
  it should "parse booleans" in {
    parse("true") should be(Success(true))
  }

  "A StringLiteralDeserializer" should "Deserialize untyped strings" in {
    StringLiteralDeserializer.deserialize(literal = "'Hello'") should be(Success("Hello"))
  }
  it should "Deserialize untyped integers" in {
    StringLiteralDeserializer.deserialize(literal = "123") should be(Success(123))
  }
  it should "Deserialize untyped doubles" in {
    StringLiteralDeserializer.deserialize(literal = "1.23") should be(Success(1.23))
  }
  it should "Deserialize untyped booleans" in {
    StringLiteralDeserializer.deserialize(literal = "true") should be(Success(true))
  }

  it should "Deserialize typed strings" in {
    StringLiteralDeserializer.deserialize(literal = "Hello", typeTag = StringLiteralDeserializer.TypeTags.STR) should be(Success("Hello"))
  }
  it should "Deserialize typed integers" in {
    StringLiteralDeserializer.deserialize(literal = "123", typeTag = StringLiteralDeserializer.TypeTags.INT) should be(Success(123))
  }
  it should "Deserialize typed floats" in {
    StringLiteralDeserializer.deserialize(literal = "1.23", typeTag = StringLiteralDeserializer.TypeTags.DBL) should be(Success(1.23))
  }
  it should "Deserialize typed booleans" in {
    StringLiteralDeserializer.deserialize(literal = "true", typeTag = StringLiteralDeserializer.TypeTags.BOL) should be(Success(true))
  }
  it should "Deserialize typed longs" in {
    StringLiteralDeserializer.deserialize(literal = "123", typeTag = StringLiteralDeserializer.TypeTags.LNG) should be(Success(123L))
  }
  it should "Deserialize typed UUIDs" in {
    StringLiteralDeserializer.deserialize(literal = "067e6162-3b6f-4ae2-a171-2470b63dff00", typeTag = StringLiteralDeserializer.TypeTags.UID) should be(
      Success(UUID.fromString("067e6162-3b6f-4ae2-a171-2470b63dff00")))
  }
  it should "Deserialize typed Dates" in {
    StringLiteralDeserializer.deserialize(literal = "2014-11-25T09:20:43.000",
      typeTag = StringLiteralDeserializer.TypeTags.DAT).get.asInstanceOf[Date].getTime() should be(1416907243000L)
  }

}
