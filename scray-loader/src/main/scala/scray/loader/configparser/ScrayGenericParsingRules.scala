package scray.loader.configparser

import org.parboiled2._

/**
 * some generic parsing stuff for parsing whitespace and principal types
 */
abstract class ScrayGenericParsingRules extends Parser {
  
  // implicitly add whitespace handling for literals
  implicit def wspStr(s: String): Rule0 = rule { str(s) ~ zeroOrMore(WhitespaceChars) }

  /* --------------------------------- generic parsing rules ------------------------------------ */
  def Identifier: Rule1[String] = rule { capture(oneOrMore(CharPredicate.AlphaNum)) ~ zeroOrMore(WhitespaceChars) }
  def QuotedString: Rule1[String] = rule { '"' ~ capture(oneOrMore(QuotedValueChars)) ~ '"' ~ zeroOrMore(WhitespaceChars) }
  def IntNumber: Rule1[Int] = rule { StringNumber ~> { (number: String) => number.toInt }}
  def StringNumber: Rule1[String] = rule { capture(oneOrMore(CharPredicate.Digit)) ~ zeroOrMore(WhitespaceChars) }
  val QuotedValueChars = CharPredicate.Printable -- '\u0022'
  val WhitespaceChars = CharPredicate.Empty ++ ' ' ++ "\n" ++ "\t"
}