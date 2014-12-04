// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package scray.core.service

import java.util.UUID
import scala.util.{ Failure, Success, Try }
import scala.math.ScalaNumericAnyConversions
import org.parboiled2._
import scray.common.serialization.StringLiteralDeserializer
import scray.querying.Query
import scray.querying.description._
import scray.querying.queries.SimpleQuery
import scray.service.qmodel.thrifscala.ScrayTQuery

/**
 * Thrift query parser resulting in an SQuery model
 */
class TQueryParser(tQuery : ScrayTQuery) extends Parser {
  override val input : ParserInput = tQuery.queryExpression

  implicit def wspStr(s : String) : Rule0 = rule { str(s) ~ zeroOrMore(' ') }

  def InputLine : Rule1[SQuery] = rule { _queryStatement ~ EOI }

  // Currently unused: Rule for constructing simple queries from parsing results
  // this can be used to create engine queries (Query) directly instead of service queries (SQuery)
  def _queryBuilder : Rule1[Query] = rule { _queryStatement ~> { (q : SQuery) => q createQuery () get } }

  // main query structure
  def _queryStatement : Rule1[SQuery] = rule { _rangeStatement }
  def _rangeStatement : Rule1[SQuery] = rule { _orderbyStatement ~ optional(_range) ~> { (q : SQuery, o : Option[SRange]) => q addRange o get } }
  def _orderbyStatement : Rule1[SQuery] = rule { _groupbyStatement ~ optional(_orderby) ~> { (q : SQuery, o : Option[SOrdering]) => q addOrdering o get } }
  def _groupbyStatement : Rule1[SQuery] = rule { _predicateStatement ~ optional(_groupby) ~> { (q : SQuery, g : Option[SGrouping]) => q addGrouping g get } }
  def _predicateStatement : Rule1[SQuery] = rule { _tableStatement ~ optional("WHERE" ~ _predicate) ~> { (q : SQuery, p : Option[SPredicate]) => q addPredicate (p) get } }
  def _tableStatement : Rule1[SQuery] = rule { _columnStatement ~ "FROM" ~ _table ~> { (q : SQuery, t : STable) => q addTable t get } }
  def _columnStatement : Rule1[SQuery] = rule { _rootStatement ~ "SELECT" ~ _columns ~> { (q : SQuery, c : SColumns) => q addColumns c get } }
  def _rootStatement : Rule1[SQuery] = rule { push(SQuery(Map[Class[_ <: SQueryComponent], SQueryComponent]())(tQuery)) }

  // Rules matching ranges
  def _range : Rule1[SRange] = rule { (_limit ~ push(None) | push(None) ~ _skip | _limit ~ _skip) ~> { (l : Option[String], s : Option[String]) => SRange(s, l) } }
  def _limit : Rule1[Option[String]] = rule { "LIMIT" ~ _number ~> { (s : String) => Some(s) } }
  def _skip : Rule1[Option[String]] = rule { "SKIP" ~ _number ~> { (s : String) => Some(s) } }

  // Rules matching 'post predicates'
  def _groupby : Rule1[SGrouping] = rule { "GROUPBY" ~ _identifier ~> { (nam : String) => SGrouping(nam) } }
  def _orderby : Rule1[SOrdering] = rule { "ORDERBY" ~ _identifier ~> { (nam : String) => SOrdering(nam) } }

  // Rules matching columns
  def _columns : Rule1[SColumns] = rule { _asterixColumn | _columnSet }
  def _asterixColumn : Rule1[SAsterixColumn] = rule { "*" ~ push(SAsterixColumn()) }
  def _columnSet : Rule1[SColumnSet] = rule { _column ~ zeroOrMore("," ~ _column) ~> { (col : SColumn, cols : Seq[SColumn]) => SColumnSet(col :: cols.toList) } }
  def _column : Rule1[SColumn] = rule { _refColum | _specColumn }
  def _specColumn : Rule1[SSpecColumn] = rule { _typedColumn | _untypedColumn }
  def _typedColumn : Rule1[SSpecColumn] = rule { _typetag ~ _identifier ~> { (tag : String, id : String) => SSpecColumn(id, Some(tag)) } }
  def _untypedColumn : Rule1[SSpecColumn] = rule { _identifier ~> { (id : String) => SSpecColumn(id, None) } }
  def _refColum : Rule1[SRefColumn] = rule { _reference ~> { (ref : String) => SRefColumn(ref) } }

  // Rules matching the table part - this might throw
  def _table : Rule1[STable] = rule { (_tableRef | _tableSpec) }
  def _tableSpec : Rule1[SSpecTable] = rule { _tableSyntax ~> { (a : String, b : String, c : String) => SSpecTable(a, b, c) } }
  def _tableSyntax = rule { "(" ~ "dbSystem" ~ "=" ~ _identifier ~ "," ~ "dbId" ~ "=" ~ _identifier ~ "," ~ "tabId" ~ "=" ~ _identifier ~ ")" }
  def _tableRef : Rule1[SRefTable] = rule { _reference ~> { (ref : String) => SRefTable(ref) } }

  // Rules matching complex predicates in disjunctive cannonical form
  def _predicate : Rule1[SPredicate] = rule { _disjunction | _conjunction | _factor }
  def _disjunction : Rule1[SPredicate] = rule { _term ~ oneOrMore("OR" ~ _term) ~> { (pre1 : SPredicate, pre2 : Seq[SPredicate]) => SOr(pre1 :: pre2.toList) } }
  def _term : Rule1[SPredicate] = rule { _conjunction | _factor }
  def _conjunction : Rule1[SPredicate] = rule { _factor ~ oneOrMore("AND" ~ _factor) ~> { (pre1 : SPredicate, pre2 : Seq[SPredicate]) => SAnd(pre1 :: pre2.toList) } }
  def _factor : Rule1[SPredicate] = rule { _parens | _atomicPredicate }
  def _parens : Rule1[SPredicate] = rule { "(" ~ (_conjunction | _disjunction | _factor) ~ ")" }

  // Rules matching atomic predicates
  def _atomicPredicate : Rule1[SAtomicPredicate] = rule { _equal | _greaterEqual | _smallerEqual | _greater | _smaller }
  def _equal : Rule1[SEqual] = rule { _identifier ~ "=" ~ _value ~> { SEqual(_, _) } }
  def _greater : Rule1[SGreater] = rule { _identifier ~ ">" ~ _value ~> { SGreater(_, _) } }
  def _smaller : Rule1[SSmaller] = rule { _identifier ~ "<" ~ _value ~> { SSmaller(_, _) } }
  def _greaterEqual : Rule1[SGreaterEqual] = rule { _identifier ~ ">=" ~ _value ~> { SGreaterEqual(_, _) } }
  def _smallerEqual : Rule1[SSmallerEqual] = rule { _identifier ~ "<=" ~ _value ~> { SSmallerEqual(_, _) } }

  // rules matching values - these might throw
  def _value : Rule1[SValue] = rule { _typedLiteralValue | _referencedValue | _plainLiteralValue }
  def _typedLiteralValue : Rule1[SLitVal] = rule { _typedLiteral ~> { (tag : String, lit : String) => SLitVal(lit, Some(tag)) } }
  def _plainLiteralValue : Rule1[SLitVal] = rule { _untypedLiteral ~> { (lit : String) => SLitVal(lit, None) } }
  def _referencedValue : Rule1[SRefVal] = rule { _reference ~> { (ref : String) => SRefVal(ref) } }

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
