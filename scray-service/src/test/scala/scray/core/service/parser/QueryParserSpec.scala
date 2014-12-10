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

import scala.Option.option2Iterable
import scala.util.Failure
import scala.util.Success
import org.junit.runner.RunWith
import org.parboiled2.ParseError
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import scray.core.service.ScrayServiceException
import scray.querying.description.And
import scray.querying.description.Columns
import scray.querying.description.Equal
import scray.querying.description.Greater
import scray.querying.description.GreaterEqual
import scray.querying.description.Smaller
import scray.querying.description.SmallerEqual
import scala.util.Try
import scray.querying.description.Or
import scray.querying.Query
import org.scalatest.junit.JUnitRunner
import scray.core.service.util.TQuerySamples

@RunWith(classOf[JUnitRunner])
class QueryParserSpec extends FlatSpec with Matchers with TQuerySamples {

  val DEBUG = false

  private def parse(query : String) = {
    val parser = new TQueryParser(createTQuery(query))
    val parsed = parser.InputLine.run() match {
      case Success(result) => Success(result)
      case Failure(e : ParseError) =>
        sys.error(parser.formatError(e, showTraces = true)); Failure(e)
      case Failure(e) => throw e
    }
    if (DEBUG) println(s"Parsed '$query' ... $parsed.get")
    parsed
  }

  private def generate(parsed : Try[_Query]) : Query = {
    parsed.isSuccess should be(true)
    val query = parsed.get.createQuery
    if (query.isFailure) println(query)
    query.isSuccess should be(true)
    query.get
  }

  "query parser" should "handle asterix columns" in {
    val parsed = parse("SELECT * FROM @myTableId")
    val query = generate(parsed)
    query.getResultSetColumns should be(Columns(Left(true)))
  }

  it should "handle single column refs" in {
    val parsed = parse("SELECT @col1 FROM @myTableId")
    val query = generate(parsed)
    query.getResultSetColumns.columns.right.get.size should be(1)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("col1")).size should be(1)
  }

  it should "handle multi column refs" in {
    val parsed = parse("SELECT @col1, @col2 FROM @myTableId")
    val query = generate(parsed)
    query.getResultSetColumns.columns.right.get.size should be(2)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("col1")).size should be(1)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("col2")).size should be(1)
  }

  it should "throw with unmatched column refs" in {
    an[ScrayServiceException] should be thrownBy parse("SELECT @col1, @col2, @col33 FROM @myTableId")
  }

  it should "handle single column literals" in {
    val parsed = parse("SELECT foo FROM @myTableId")
    val query = generate(parsed)
    query.getResultSetColumns.columns.right.get.size should be(1)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("foo")).size should be(1)
  }

  it should "handle multi column literals" in {
    val parsed = parse("SELECT foo, bar FROM @myTableId")
    val query = generate(parsed)
    query.getResultSetColumns.columns.right.get.size should be(2)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("foo")).size should be(1)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("bar")).size should be(1)
  }

  it should "handle single typed column literals" in {
    val parsed = parse("SELECT !!long foo FROM @myTableId")
    val query = generate(parsed)
    // no test for type meta info at this level
    query.getResultSetColumns.columns.right.get.size should be(1)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("foo")).size should be(1)
  }

  it should "handle multiple typed column literals" in {
    val parsed = parse("SELECT !!long foo, !!string bar FROM @myTableId")
    val query = generate(parsed)
    // no test for type meta info at this level
    query.getResultSetColumns.columns.right.get.size should be(2)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("foo")).size should be(1)
    query.getResultSetColumns.columns.right.get.find(_.columnName.equals("bar")).size should be(1)
  }

  it should "handle table literals" in {
    val parsed = parse("SELECT * FROM (dbSystem=foo, dbId=bar, tabId=baz)")
    val query = generate(parsed)
    query.getTableIdentifier.dbId should be("bar")
    query.getTableIdentifier.dbSystem should be("foo")
    query.getTableIdentifier.tableId should be("baz")
  }

  it should "handle table references" in {
    val parsed = parse("SELECT * FROM @myTableId")
    val query = generate(parsed)
    query.getTableIdentifier.dbId should be("myDbId")
    query.getTableIdentifier.dbSystem should be("myDbSystem")
    query.getTableIdentifier.tableId should be("myTableId")
  }

  it should "handle atomic predicates with literal values" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1=1")
    val query = generate(parsed)
    query.getWhereAST.get.asInstanceOf[Equal[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with typed literal values" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1 = !!long 2")
    val query = generate(parsed)
    query.getWhereAST.get.asInstanceOf[Equal[Long]].value.asInstanceOf[Ordered[Long]].compareTo(2L) should be(0)
  }

  it should "handle atomic predicates with 'equal'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1=1")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[Equal[_]] should be(true)
    query.getWhereAST.get.asInstanceOf[Equal[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with 'smaller'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1<1")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[Smaller[_]] should be(true)
    query.getWhereAST.get.asInstanceOf[Smaller[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with 'smaller equal'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1<=1")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[SmallerEqual[_]] should be(true)
    query.getWhereAST.get.asInstanceOf[SmallerEqual[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with 'greater'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1>1")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[Greater[_]] should be(true)
    query.getWhereAST.get.asInstanceOf[Greater[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle atomic predicates with 'greater equal'" in {
    val parsed = parse("SELECT col1 FROM @myTableId WHERE col1>=1")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[GreaterEqual[_]] should be(true)
    query.getWhereAST.get.asInstanceOf[GreaterEqual[Int]].value.asInstanceOf[Ordered[Int]].compareTo(1) should be(0)
  }

  it should "handle complex 'AND' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId WHERE col1=1 AND col2=2")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[And] should be(true)
  }

  it should "handle complex 'OR' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId WHERE col1=1 OR col2=2")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[Or] should be(true)
  }

  it should "handle mixed complex predicates as disjunctions" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId WHERE col1>1 OR col2>2 AND col1<10")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[Or] should be(true)
  }

  it should "handle nested complex predicates with parens" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId WHERE ( col1>1 OR col2>2 ) AND col1<10")
    val query = generate(parsed)
    query.getWhereAST.get.isInstanceOf[And] should be(true)
  }

  it should "handle 'ORDERBY' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId ORDERBY col2")
    val query = generate(parsed)
    query.getOrdering.isDefined should be(true)
  }

  it should "handle 'GROUPBY' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId GROUPBY col2")
    val query = generate(parsed)
    query.getGrouping.nonEmpty should be(true)
    query.getGrouping.get.column.columnName should be("col2")
  }

  it should "handle 'LIMIT' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId LIMIT 100")
    val query = generate(parsed)
    query.getQueryRange.get.limit.nonEmpty should be(true)
  }

  it should "handle 'SKIP' predicates" in {
    val parsed = parse("SELECT col1, col2 FROM @myTableId SKIP 100")
    val query = generate(parsed)
    query.getQueryRange.get.skip.nonEmpty should be(true)
  }

}
