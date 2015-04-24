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
package scray.querying.queries

import scray.querying.description.Column
import scray.querying.description.ColumnGrouping
import scray.querying.description.ColumnOrdering
import scray.querying.description.QueryRange
import scray.querying.description.TableIdentifier
import scray.querying.description.internal.Domain
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scray.querying.description.internal.QueryCostsAreTooHigh


case class DomainQuery(
    val id: UUID,
    val querySpace: String, 
    val columns: List[Column],
    val table: TableIdentifier,
    val domains: List[Domain[_]],
    val grouping: Option[ColumnGrouping],
    val ordering: Option[ColumnOrdering[_]],
    val range: Option[QueryRange]
) {
  
  val costs: AtomicInteger = new AtomicInteger()
  
  def incementRowCounter() {
    costs.incrementAndGet()
  }
  
  def getQueryID: UUID = id
  
  def getQueryspace: String = querySpace
  
  def getResultSetColumns: List[Column] = columns
  
  def getTableIdentifier: TableIdentifier = table
  
  def getWhereAST: List[Domain[_]] = domains
  
  def getGrouping: Option[ColumnGrouping] = grouping
  
  def getOrdering: Option[ColumnOrdering[_]] = ordering
  
  def getQueryRange: Option[QueryRange] = range
  
  def transformedAstCopy(ast: List[Domain[_]]): DomainQuery = this.copy(domains = ast)

  def getCosts: AtomicInteger = costs;
  
  def checkCosts { 
    val heapFreeSize =  Runtime.getRuntime().freeMemory()
    val heapSize = Runtime.getRuntime().maxMemory();
    val usableHeapPercentage = 95
    
    val maxHeapForThisQuery = ((heapSize/ 100) * usableHeapPercentage)

    if(costs.get > 300L && heapFreeSize > maxHeapForThisQuery) {
      throw new QueryCostsAreTooHigh(this);
    }
  }
}
