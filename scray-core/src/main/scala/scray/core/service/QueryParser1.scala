package scray.core.service

import org.parboiled2._

class QueryParser1(val input : ParserInput) extends Parser {

  implicit def wspStr(s : String) : Rule0 = rule {
    str(s) ~ zeroOrMore(' ')
  }

  def InputLine = rule { QueryStatement ~ EOI }

  def QueryStatement = rule {
    Select ~ Columns ~ From ~ Table ~ zeroOrMore(Where ~ oneOrMore(Predicate)) ~ zeroOrMore(Groupby) ~ zeroOrMore(Orderby)
  }

  def Select = rule { "SELECT" }

  def Columns = rule { ColId ~ zeroOrMore("," ~ ColId) }

  def ColId = rule { Identifier | Asterix }

  def From = rule { "FROM" }

  def Table = rule { Identifier }

  def Where = rule { "WHERE" }

  def Predicate = rule { AtomicPredicate ~ zeroOrMore(Operator ~ AtomicPredicate) }

  def AtomicPredicate = rule { Identifier ~ Comparator ~ Identifier }

  def Operator = rule { "AND" | "OR" }

  def Comparator = rule { "=" | "<" | "<=" | ">" | ">=" }

  def Groupby = rule { "GROUPBY" ~ Identifier }

  def Orderby = rule { "ORDERBY" ~ Identifier }

  def Identifier = rule { oneOrMore(CharPredicate.AlphaNum) ~ zeroOrMore(' ') }

  def Asterix = rule { "*" }

}
