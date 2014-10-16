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
package scray.querying

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.description.{Clause, Column, Columns, QueryRange, TableIdentifier}
import scray.querying.description.{And, Equal, Or}
import scray.querying.queries.SimpleQuery
import scray.querying.description.internal.{Bound, RangeValueDomain}

/**
 * Scray querying specification.
 */
@RunWith(classOf[JUnitRunner])
class ScrayQueryingTest extends WordSpec {
  val bla = "bla"
  val ti = TableIdentifier("cassandra", "mytestspace", "mycf")
  "Scray's querying model" should {
    "instantiate SimpleQuery" in {
      val sq = SimpleQuery("", ti,
          where = Some(And(Equal(Column("bla", ti), 2)))
          )
      assert(!sq.where.isEmpty)
    }
    "intersect 2 RangeValueDomains" in {
      val dom1 = RangeValueDomain(Column(bla, ti), Some(Bound(true, 2)), None)
      val dom2 = RangeValueDomain(Column(bla, ti), None, Some(Bound(true, 10)))
      val res = dom1.bisect(dom2)
      assert(res != None)
      assert(res.get.isInstanceOf[RangeValueDomain[_]])
      assert(res.get.lowerBound != None)
      assert(res.get.upperBound != None)
    }
    "intersect 3 RangeValueDomains with a join in the upper bound" in {
      val dom1 = RangeValueDomain(Column(bla, ti), Some(Bound(true, 2)), None)
      val dom2 = RangeValueDomain(Column(bla, ti), None, Some(Bound(true, 10)))
      val dom3 = RangeValueDomain(Column(bla, ti), None, Some(Bound(true, 8)))
      val res1 = dom1.bisect(dom2)
      val res2 = res1.get.bisect(dom3)
      assert(res2 != None)
      assert(res2.get.isInstanceOf[RangeValueDomain[_]])
      assert(res2.get.lowerBound != None)
      assert(res2.get.upperBound != None)
      assert(res2.get.upperBound.get.value == 8)
    }
    "intersect 3 RangeValueDomains with a join in the lower bound" in {
      val dom1 = RangeValueDomain(Column(bla, ti), Some(Bound(true, 2)), None)
      val dom2 = RangeValueDomain(Column(bla, ti), None, Some(Bound(true, 10)))
      val dom3 = RangeValueDomain(Column(bla, ti), Some(Bound(false, 8)), None)
      val res1 = dom1.bisect(dom2)
      val res2 = res1.get.bisect(dom3)
      assert(res2 != None)
      assert(res2.get.isInstanceOf[RangeValueDomain[_]])
      assert(res2.get.lowerBound != None)
      assert(res2.get.upperBound != None)
      assert(res2.get.lowerBound.get.value == 8)
    }
    "intersect 2 RangeValueDomains with a join in the lower bound" in {
      val dom1 = RangeValueDomain(Column(bla, ti), Some(Bound(true, 2)), None)
      val dom2 = RangeValueDomain(Column(bla, ti), Some(Bound(false, 8)), None)
      val res = dom1.bisect(dom2)
      assert(res != None)
      assert(res.get.isInstanceOf[RangeValueDomain[_]])
      assert(res.get.lowerBound != None)
      assert(res.get.upperBound == None)
      assert(res.get.lowerBound.get.value == 8)
    }
    "intersect 2 RangeValueDomains with a join in the upper bound" in {
      val dom1 = RangeValueDomain(Column(bla, ti), None, Some(Bound(true, 2)))
      val dom2 = RangeValueDomain(Column(bla, ti), None, Some(Bound(false, 8)))
      val res = dom1.bisect(dom2)
      assert(res != None)
      assert(res.get.isInstanceOf[RangeValueDomain[_]])
      assert(res.get.lowerBound == None)
      assert(res.get.upperBound != None)
      assert(res.get.upperBound.get.value == 2)
    }
  }
}
