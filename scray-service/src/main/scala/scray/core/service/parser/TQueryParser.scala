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

package scray.core.service.parser

import org.parboiled2._
import scray.querying.Query
import scray.querying.description._
import scray.service.qmodel.thrifscala.ScrayTQuery
import org.parboiled2.ParserInput.apply
import com.typesafe.scalalogging.slf4j.LazyLogging

/**
 * Thrift query parser resulting in a _Query model
 */
class TQueryParser(tQuery: ScrayTQuery) extends Parser with LazyLogging {
  override val input: ParserInput = tQuery.queryExpression

  implicit def wspStr(s: String): Rule0 = rule { str(s) ~ zeroOrMore(' ') }

  def InputLine: Rule1[_Query] = rule { _queryStatement.named("AndyStatement") ~ EOI }

  // Currently unused: Rule for constructing simple queries from parsing results
  // this can be used to create engine queries (Query) directly instead of service queries (_Query)
  def _queryBuilder: Rule1[Query] = rule { _queryStatement ~> { (q: _Query) => q createQuery () get } }

  // main query structure
  def _queryStatement: Rule1[_Query] = rule { _rangeStatement }
  def _rangeStatement: Rule1[_Query] = rule { _orderbyStatement ~ optional(_range) ~> { (q: _Query, o: Option[_Range]) => q addRange o get } }
  def _orderbyStatement: Rule1[_Query] = rule { _groupbyStatement ~ optional(_orderby) ~> { (q: _Query, o: Option[_Ordering]) => q addOrdering o get } }
  def _groupbyStatement: Rule1[_Query] = rule { _predicateStatement ~ optional(_groupby) ~> { (q: _Query, g: Option[_Grouping]) => q addGrouping g get } }
  def _predicateStatement: Rule1[_Query] = rule { _tableStatement ~ optional("WHERE" ~ _predicate) ~> { (q: _Query, p: Option[_Predicate]) => q addPredicate (p) get } }
  def _tableStatement: Rule1[_Query] = rule { _columnStatement ~ "FROM" ~ _table ~> { (q: _Query, t: _Table) => q addTable t get } }
  def _columnStatement: Rule1[_Query] = rule { _rootStatement ~ "SELECT" ~ _columns ~> { (q: _Query, c: _Columns) => q addColumns c get } }
  def _rootStatement: Rule1[_Query] = rule { push(_Query(Map[Class[_ <: _QueryComponent], _QueryComponent]())(tQuery)) }

  // Rules matching ranges
  def _range: Rule1[_Range] = rule { (_skip ~ _limit | push(None) ~ _limit | _skip ~ push(None)) ~> { (s: Option[String], l: Option[String]) => _Range(s, l) } }
  def _limit: Rule1[Option[String]] = rule { "LIMIT" ~ _number ~> { (s: String) => Some(s) } }
  def _skip: Rule1[Option[String]] = rule { "SKIP" ~ _number ~> { (s: String) => Some(s) } }

  // Rules matching 'post predicates'
  def _groupby: Rule1[_Grouping] = rule { "GROUP BY" ~ _identifier ~> { (nam: String) => _Grouping(nam) } }
  def _orderby: Rule1[_Ordering] = rule { "ORDER BY" ~ _identifier ~ optional(capture("DESC") | capture("ASC")) ~> { (nam: String, desc: Option[String]) => _Ordering(nam, desc.map(_.trim.equals("DESC")).getOrElse(false)) } }

  // Rules matching columns
  def _columns: Rule1[_Columns] = rule { _asterixColumn | _columnSet }
  def _asterixColumn: Rule1[_AsterixColumn] = rule { "*" ~ push(_AsterixColumn()) }
  def _columnSet: Rule1[_ColumnSet] = rule { _column ~ zeroOrMore("," ~ _column) ~> { (col: _Column, cols: Seq[_Column]) => _ColumnSet(col :: cols.toList) } }
  def _column: Rule1[_Column] = rule { _refColum | _specColumn }
  def _specColumn: Rule1[_SpecColumn] = rule { _typedColumn | _untypedColumn }
  def _typedColumn: Rule1[_SpecColumn] = rule { _typetag ~ _identifier ~> { (tag: String, id: String) => _SpecColumn(id, Some(tag)) } }
  def _untypedColumn: Rule1[_SpecColumn] = rule { _identifier ~> { (id: String) => _SpecColumn(id, None) } }
  def _refColum: Rule1[_RefColumn] = rule { _reference ~> { (ref: String) => _RefColumn(ref) } }

  // Rules matching the table part - this might throw
  def _table: Rule1[_Table] = rule { (_tableRef | _tableSpec) }
  def _tableSpec: Rule1[_SpecTable] = rule { _tableSyntax ~> { (a: String, b: String, c: String) => _SpecTable(a, b, c) } }
  def _tableSyntax = rule { "(" ~ "dbSystem" ~ "=" ~ _identifier ~ "," ~ "dbId" ~ "=" ~ _identifier ~ "," ~ "tabId" ~ "=" ~ _identifier ~ ")" }
  def _tableRef: Rule1[_RefTable] = rule { _reference ~> { (ref: String) => _RefTable(ref) } }

  // Rules matching complex predicates in disjunctive cannonical form
  def _predicate: Rule1[_Predicate] = rule { _disjunction | _conjunction | _factor }
  def _disjunction: Rule1[_Predicate] = rule { _term ~ oneOrMore("OR" ~ _term) ~> { (pre1: _Predicate, pre2: Seq[_Predicate]) => _Or(pre1 :: pre2.toList) } }
  def _term: Rule1[_Predicate] = rule { _conjunction | _factor }
  def _conjunction: Rule1[_Predicate] = rule { _factor ~ oneOrMore("AND" ~ _factor) ~> { (pre1: _Predicate, pre2: Seq[_Predicate]) => _And(pre1 :: pre2.toList) } }
  def _factor: Rule1[_Predicate] = rule { _parens | _atomicPredicate }
  def _parens: Rule1[_Predicate] = rule { "(" ~ (_conjunction | _disjunction | _factor) ~ ")" }

  // Rules matching atomic predicates
  def _atomicPredicate: Rule1[_AtomicPredicate] = rule { _equal | _greaterEqual | _smallerEqual | _greater | _smaller | _unequal | _isnull }
  def _equal: Rule1[_Equal] = rule { _identifier ~ "=" ~ _value ~> { _Equal(_, _) } }
  def _greater: Rule1[_Greater] = rule { _identifier ~ ">" ~ _value ~> { _Greater(_, _) } }
  def _smaller: Rule1[_Smaller] = rule { _identifier ~ "<" ~ _value ~> { _Smaller(_, _) } }
  def _greaterEqual: Rule1[_GreaterEqual] = rule { _identifier ~ ">=" ~ _value ~> { _GreaterEqual(_, _) } }
  def _smallerEqual: Rule1[_SmallerEqual] = rule { _identifier ~ "<=" ~ _value ~> { _SmallerEqual(_, _) } }
  def _unequal: Rule1[_Unequal] = rule { _identifier ~ "<>" ~ _value ~> { _Unequal(_, _) }}
  def _isnull: Rule1[_IsNull] = rule { _identifier ~ "IS NULL" ~> { _IsNull(_) }}

  // rules matching values - these might throw
  def _value: Rule1[_Value] = rule { _typedLiteralValue.named("TypedLiteral") | _referencedValue.named("Reference") | _plainLiteralValue.named("PlainValue") }
  def _typedLiteralValue: Rule1[_LitVal] = rule { _typedLiteral ~> { (tag: String, lit: String) => _LitVal(lit, Some(tag)) } }
  def _plainLiteralValue: Rule1[_LitVal] = rule { _untypedLiteral ~> { (lit: String) => _LitVal(lit, None) } }
  def _referencedValue: Rule1[_RefVal] = rule { _reference ~> { (ref: String) => _RefVal(ref) } }

  // Rules matching value literals
  def _typedLiteral: Rule2[String, String] = rule { _typetag ~ { _quotedValueChars | _unquotedValueChars } }
  def _untypedLiteral: Rule1[String] = rule { _emptyQuotes.named("EmptyQuotes") | _quotedValueChars.named("Quoted") | _unquotedValueChars.named("Unquoted") }

  def _unquotedValueChars: Rule1[String] = rule { capture(oneOrMore(UnquotedValueChars)) ~ zeroOrMore(' ') }
  
  def _emptyQuotes: Rule1[String] = rule { capture(str("''")) ~ zeroOrMore(' ') ~> { (a:String) => a } }
  def _quotedValueChars: Rule1[String] = rule { capture(str("'")) ~ capture(zeroOrMore(QuotedValueChars)) ~ capture(str("'")) ~ zeroOrMore(' ') ~> { (a: String, b: String, c: String) => a + b + c } }

  val QuotedValueChars = CharPredicate.Printable -- '\u0027'
  val UnquotedValueChars = CharPredicate.AlphaNum ++ '.' ++ '-' ++ '+'

  // Rules matching terminals
  def _typetag: Rule1[String] = rule { capture(str("!!") ~ oneOrMore(CharPredicate.AlphaNum)) ~ oneOrMore(' ') } // tag w/ '!!'
  def _reference: Rule1[String] = rule { str("@") ~ _identifier } // ref w/o '@'
  def _identifier: Rule1[String] = rule { capture(oneOrMore(CharPredicate.AlphaNum)) ~ zeroOrMore(' ') }
  def _number: Rule1[String] = rule { capture(oneOrMore(CharPredicate.Digit)) ~ zeroOrMore(' ') }
}
