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
import scray.querying.source.SimpleHashJoinSource

class LinearSimpleHashJoinCosts[Q <: DomainQuery, K <: DomainQuery, V](source: SimpleHashJoinSource[Q, K, V], factory: QueryCostFunctionFactory) extends CostFunction[Q] {
  def apply(query: Q): QueryCosts = {
    val sourceCosts = source.source.getCosts(query)(factory)
    val lookupCosts = source.lookupSource.getCosts(source.getKeyedQuery(query, Set()))(factory)
    val amount = sourceCosts.estimatedAmount + sourceCosts.estimatedCardinality * lookupCosts.estimatedAmount
    QueryCosts(amount, sourceCosts.estimatedCardinality)
  }
  def getCosts(query: Q): QueryCosts = apply(query)
}