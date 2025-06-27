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
package scray.querying.source.costs

import scray.querying.queries.DomainQuery
import scray.querying.source.Source
import scray.querying.source.SimpleHashJoinSource

object LinearQueryCostFuntionFactory {

  implicit object defaultFactory extends QueryCostFunctionFactory {
    override def getCostFunction[Q <: DomainQuery, T](source: Source[Q, T]): CostFunction[Q] = source match {
      case hashJoinSource: SimpleHashJoinSource[a, b, c] => new LinearSimpleHashJoinCosts(hashJoinSource, this)
      //case keyValueSource: KeyValueSource[k,v] => new KeyValueSourceCosts(keyValueSource, this)
    }
  }
}