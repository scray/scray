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
package scray.querying.planning

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.description.{And, Or}
import scray.querying.description.Column
import scray.querying.description.Equal
import scray.querying.description.TableIdentifier
import scray.querying.queries.SimpleQuery
import scray.querying.description.internal.QueryDomainParserException
import scray.querying.description.Greater
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.Smaller
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.SmallerEqual

/**
 * Scray querying specification.
 */
@RunWith(classOf[JUnitRunner])
class ScrayQueryingPlannerTest extends WordSpec {
  val ti = TableIdentifier("cassandra", "mytestspace", "mycf")
  val planner = new Planner
  "Scray's select-project-join planner" should {
    "flatten nested 'AND's" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(And(Equal(Column("bla1", ti), 1),Equal(Column("bla2", ti), 2))))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 1)
      assert(result.head.getWhereAST.get.isInstanceOf[And])
      assert(!result.head.getWhereAST.get.asInstanceOf[And].clauses.head.isInstanceOf[And])
    }
    "flatten nested 'OR's" in {
      val sq = SimpleQuery("", ti,
          where = Some(Or(Or(Equal(Column("bla1", ti), 1))))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 1)
      assert(!result.head.getWhereAST.get.isInstanceOf[Or])
    }
    "divide 'OR's into multiple queries" in {
      val sq = SimpleQuery("", ti,
          where = Some(Or(Equal(Column("bla", ti), 2),Equal(Column("bla2", ti), 2)))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 2)
      assert(result.head.getWhereAST.get.isInstanceOf[Equal[_]])
    }
    "use distributive law for multiplying/anding one 'OR's within one 'AND'" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Equal(Column("bla1", ti), 1),
              Or(Equal(Column("bla", ti), 2),Equal(Column("bla2", ti), 2))))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 2)
      assert(result.head.getWhereAST.get.isInstanceOf[And])
      assert(result.tail.head.getWhereAST.get.isInstanceOf[And])
    }
    "use distributive law for multiplying/anding two 'OR's within one 'AND'" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Or(Equal(Column("bla11", ti), 1), Equal(Column("bla12", ti), 1)),
              Or(Equal(Column("bla21", ti), 2),Equal(Column("bla22", ti), 2))))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 4)
      assert(result.head.getWhereAST.get.isInstanceOf[And])
      assert(result.tail.head.getWhereAST.get.asInstanceOf[And].clauses.size == 2)
    }
    "use distributive law for multiplying/anding three 'OR's within one 'AND'" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Or(Equal(Column("bla11", ti), 1), Equal(Column("bla12", ti), 1)),
              Or(Equal(Column("bla21", ti), 2),Equal(Column("bla22", ti), 2)),
              Or(Equal(Column("bla31", ti), 3),Equal(Column("bla32", ti), 3))))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 8)
      assert(result.head.getWhereAST.get.isInstanceOf[And])
      assert(result.tail.head.getWhereAST.get.asInstanceOf[And].clauses.size == 3)
    }
    "use distributive law for multiplying/anding two 'OR's and one 'AND' within one 'AND'" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Or(Equal(Column("bla11", ti), 1), Equal(Column("bla12", ti), 1)),
              And(Equal(Column("bla21", ti), 2),Equal(Column("bla22", ti), 2)),
              Or(Equal(Column("bla31", ti), 3),Equal(Column("bla32", ti), 3))))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 4)
      assert(result.head.getWhereAST.get.isInstanceOf[And])
      assert(result.tail.head.getWhereAST.get.asInstanceOf[And].clauses.size == 4)
    }
    "use distributive law for multiplying/anding two 'OR's with a nested 'AND' within one 'AND'" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Or(Equal(Column("bla11", ti), 1), Equal(Column("bla12", ti), 1)),
              Or(Equal(Column("bla21", ti), 2),And(Equal(Column("bla22", ti), 2), Equal(Column("bla23", ti), 2)))))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 4)
      assert(result.head.getWhereAST.get.isInstanceOf[And])
      assert(result.head.getWhereAST.get.asInstanceOf[And].clauses.size == 2)
      val rest = result.tail
      assert(rest.head.getWhereAST.get.isInstanceOf[And])
      assert(rest.head.getWhereAST.get.asInstanceOf[And].clauses.size == 3)
    }
    "use distributive law for multiplying/anding two 'OR's with a nested 'AND' with a nested 'OR' within one 'AND'" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Or(Equal(Column("bla11", ti), 1), Equal(Column("bla12", ti), 1)),
              Or(Equal(Column("bla21", ti), 2),And(Equal(Column("bla31", ti), 3), 
                  Or(Equal(Column("bla41", ti), 4), Equal(Column("bla42", ti), 4))))))
          )
      val result = planner.distributiveOrReductionToConjunctiveQuery(sq)
      assert(result.size == 6)
      assert(result.head.getWhereAST.get.isInstanceOf[And])
      assert(result.head.getWhereAST.get.asInstanceOf[And].clauses.size == 2)
      val rest = result.tail
      assert(rest.head.getWhereAST.get.isInstanceOf[And])
      assert(rest.head.getWhereAST.get.asInstanceOf[And].clauses.size == 3)
    }
  }
  "Scray's unification of domains in the planner" should {
    "leave two predicates with disjoint columns" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Equal(Column("bla11", ti), 1), Equal(Column("bla12", ti), 1)))
          )
      val result = planner.qualifyPredicates(sq)
      assert(result != None)
      assert(result.get.size == 2)
    }
    "join two predicates with the same column and same value" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Equal(Column("bla11", ti), 1), Equal(Column("bla11", ti), 1)))
          )
      val result = planner.qualifyPredicates(sq)
      assert(result != None)
      assert(result.get.size == 1)
    }
    "detect a conflict on two predicates with the same column and not the same value" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Equal(Column("bla11", ti), 1), Equal(Column("bla11", ti), 2)))
          )
      intercept[QueryDomainParserException] {
        planner.qualifyPredicates(sq)
      }
    }
    "join two predicates with the same column and a value within a lower bound" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Equal(Column("bla11", ti), 2), Greater(Column("bla11", ti), 1)))
          )
      val result = planner.qualifyPredicates(sq)
      assert(result != None)
      assert(result.get.size == 1)
      assert(result.get.head.isInstanceOf[SingleValueDomain[_]])
    }
    "detect a conflict on two predicates with the same column and a domain conflict" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Greater(Column("bla11", ti), 2), Equal(Column("bla11", ti), 1)))
          )
      intercept[QueryDomainParserException] {
        planner.qualifyPredicates(sq)
      }
    }
    "join two predicates with the same column specifying a range" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Greater(Column("bla11", ti), 1), Smaller(Column("bla11", ti), 2)))
          )
      val result = planner.qualifyPredicates(sq)
      assert(result != None)
      assert(result.get.size == 1)
      assert(result.get.head.isInstanceOf[RangeValueDomain[_]])
    }
    "detect a conflict on two predicates with the same column and same value but > and <" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Greater(Column("bla11", ti), 2), Smaller(Column("bla11", ti), 2)))
          )
      intercept[QueryDomainParserException] {
        planner.qualifyPredicates(sq)
      }
    }
    "join three predicates with the same column specifying a range" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Greater(Column("bla11", ti), 1), Smaller(Column("bla11", ti), 5), SmallerEqual(Column("bla11", ti), 3)))
          )
      val result = planner.qualifyPredicates(sq)
      assert(result != None)
      assert(result.get.size == 1)
      assert(result.get.head.isInstanceOf[RangeValueDomain[_]])
      assert(result.get.head.asInstanceOf[RangeValueDomain[Int]].upperBound.get.value == 3)
    }
  }
//  "Scray's result-set column identifier" should {
//    "find columns from tables and domains" in {
//      val sq = SimpleQuery("", ti,
//          where = Some(And(Equal(Column("bla11", ti), 1), Equal(Column("bla12", ti), 1)))
//          )
//      val result = planner.qualifyPredicates(sq)
//      assert(result != None)
//      assert(result.get.size == 2)
//    }

//  "Scray's identification of queried columns in the planner" should {
//    "be able to identify all columns with a * query" in {
//
}
