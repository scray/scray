package scray.common.serialization

import java.text.SimpleDateFormat
import java.util.UUID
import scala.util._
import org.parboiled2._
import scray.common.exceptions._
import shapeless._
import java.util.Date

/**
 * Utility object transforming human readable string literals into corresponding typed data.
 *
 * Literals can be typed via YAML-like tags or untyped.
 *
 * Untyped literals transform as follows:
 * - alphanumeric strings in simple quotes into string objects
 * - numeric strings into integers
 * - numeric strings containing a dot into doubles
 *
 */
object StringLiteralDeserializer {

  /**
   *  Type tag constants
   */
  object TypeTags {
    val STR = "!!string"
    val INT = "!!int"
    val LNG = "!!long"
    val DBL = "!!double"
    val BOL = "!!bool"
    val UID = "!!uuid"
    val DAT = "!!date"
  }

  // date type constants
  val DATEFORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"
  val TIMEZONE = "GMT"

  /**
   * Generated literal parser
   */
  class LiteralParser(override val input : ParserInput) extends Parser {
    def InputLine = rule { literal ~ EOI }
    def literal : Rule1[Any] = rule { str | float | int | bool }
    def str : Rule1[String] = rule { ''' ~ capture(oneOrMore(CharPredicate.AlphaNum)) ~ ''' }
    def float : Rule1[Double] = rule { capture(oneOrMore(CharPredicate.Digit) ~ '.' ~ oneOrMore(CharPredicate.Digit)) ~> { (in : String) => in.toDouble } }
    def int : Rule1[Int] = rule { capture(oneOrMore(CharPredicate.Digit)) ~> { (in : String) => in.toInt } }
    def bool : Rule1[Boolean] = rule { capture("true" | "false") ~> { (in : String) => in.toBoolean } }
  }

  /**
   * Deserialize untyped literals
   */
  def deserialize(literal : String) : Try[Any] = {
    val parser = new LiteralParser(literal)
    parser.InputLine.run() match {
      case Success(result) => Success(result)
      case Failure(e) => {
        if (e.isInstanceOf[ParseError]) sys.error(parser.formatError(e.asInstanceOf[ParseError], showTraces = true))
        Failure(new ScrayServiceException(id = ExceptionIDs.PARSING_ERROR, None, msg = s"Unparsable value literal '$literal'.", cause = Some(e)))
      }
    }
  }

  /**
   * Deserialize typed literals
   */
  def deserialize(literal : String, typeTag : String) : Try[Any] = typeTag match {
    case tag : String if (tag.equals(TypeTags.STR)) => Success(literal)
    case tag : String if (tag.equals(TypeTags.INT)) => cast[Int]("Int", literal, { _.toInt })
    case tag : String if (tag.equals(TypeTags.LNG)) => cast[Long]("Long", literal, { _.toLong })
    case tag : String if (tag.equals(TypeTags.DBL)) => cast[Double]("Double", literal, { _.toDouble })
    case tag : String if (tag.equals(TypeTags.BOL)) => cast[Boolean]("Boolean", literal, { _.toBoolean })
    case tag : String if (tag.equals(TypeTags.UID)) => cast[UUID]("UUID", literal, { UUID.fromString(_) })
    case tag : String if (tag.equals(TypeTags.DAT)) => cast[Date]("Date", literal, {
      val sdf = new SimpleDateFormat(DATEFORMAT)
      sdf.setTimeZone(java.util.TimeZone.getTimeZone(TIMEZONE))
      sdf.parse(_)
    })
  }

  /**
   * Generic string casting function
   */
  def cast[T](typeName : String, literal : String, castfun : (String) => T) : Try[T] = try {
    val res = castfun(literal)
    Success(res)
  } catch {
    case e : Exception => Failure(
      new ScrayServiceException(
        id = ExceptionIDs.PARSING_ERROR, None, msg = s"Unparsable ${typeName} literal '$literal'.", cause = Some(e)))
  }

}