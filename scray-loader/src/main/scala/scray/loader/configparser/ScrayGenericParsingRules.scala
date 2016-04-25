package scray.loader.configparser

import org.parboiled2._
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit
import scray.loader.UnknownTimeUnitException

/**
 * some generic parsing stuff for parsing whitespace and principal types
 */
abstract class ScrayGenericParsingRules extends Parser {
  
  // implicitly add whitespace handling for literals
  implicit def wspStr(s: String): Rule0 = rule { str(s) ~ zeroOrMore(WhitespaceChars) }

  /* --------------------------------- parse Durations ------------------------------------------ */
  def DurationRule: Rule1[Duration] = rule { LongNumber ~ Timeunit ~> { (amount: Long, unit: TimeUnit) => Duration(amount, unit) }} 
  def Timeunit: Rule1[TimeUnit] = rule { Identifier ~> { (timeunit: String) => timeunit.toUpperCase() match {
    case "S" | "SECONDS" | "SECOND" => TimeUnit.SECONDS
    case "MS" | "MILLISECONDS" | "MILLISECOND" | "MILLIS" | "MILLI" => TimeUnit.MILLISECONDS
    case "MICROSECONDS" | "MICROSECOND" | "MICROS" | "MICRO" => TimeUnit.MICROSECONDS
    case "NS" | "NANOSECONDS" | "NANOSECOND" | "NANOS" | "NANO" => TimeUnit.NANOSECONDS
    case "D" | "DAYS" | "DAY" => TimeUnit.DAYS
    case "H" | "HOURS" | "HOUR" => TimeUnit.HOURS
    case "M" | "MINUTES" | "MINUTE" => TimeUnit.MINUTES
    case _ => throw new UnknownTimeUnitException(timeunit)
  }}}
  
  /* --------------------------------- generic parsing rules ------------------------------------ */
  def Identifier: Rule1[String] = rule { capture(oneOrMore(CharPredicate.AlphaNum)) ~ zeroOrMore(WhitespaceChars) }
  def QuotedString: Rule1[String] = rule { '"' ~ capture(oneOrMore(QuotedValueChars)) ~ '"' ~ zeroOrMore(WhitespaceChars) }
  def IntNumber: Rule1[Int] = rule { LongNumber ~> { (number: Long) => number.toInt }}
  def LongNumber: Rule1[Long] = rule { StringNumber ~> { (number: String) => number.toLong }}
  def StringNumber: Rule1[String] = rule { capture(oneOrMore(CharPredicate.Digit)) ~ zeroOrMore(WhitespaceChars) }
  val QuotedValueChars = CharPredicate.Printable -- '\u0022'
  val WhitespaceChars = CharPredicate.Empty ++ ' ' ++ "\n" ++ "\t"
}