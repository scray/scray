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

import scray.querying.description.{ Clause, Column, Columns, Equal, TableIdentifier }
import scray.querying.description.internal.Domain
import scray.querying.description.internal.SingleValueDomain
import java.util.UUID
import scray.querying.description.internal.SingleValueDomain
import scray.querying.description.internal.KeyBasedQueryException

/**
 * query to look up a primary key in a table
 */
class KeyBasedQuery[K](val key: K, reftable: TableIdentifier, result: List[Column], space: String, version: Int, qid: UUID) 
  extends DomainQuery(qid, space, version, result, reftable, List(), None, None, None) {
  
  override def transformedAstCopy(ast: List[Domain[_]]): KeyBasedQuery[K] = ast.headOption.map {
    _ match {
      case svd: SingleValueDomain[K] => new KeyBasedQuery[K](svd.value, reftable, result, space, version, qid)
      case _ => throw new KeyBasedQueryException(this)
    }
  }.orElse(throw new KeyBasedQueryException(this)).get
}


/**
 * query to look up a set of primary keys in a table
 */
class KeySetBasedQuery[K](override val key: Set[K], reftable: TableIdentifier, result: List[Column], space: String, version: Int, qid: UUID) 
  extends KeyBasedQuery[Set[K]](key, reftable, result, space, version, qid) {

  override def transformedAstCopy(ast: List[Domain[_]]): KeySetBasedQuery[K] = new KeySetBasedQuery[K](ast.collect {
    case svd: SingleValueDomain[K] => svd.value
  }.toSet, reftable, result, space, version, qid)
  
  def getAsKeyBasedQueries(): Set[KeyBasedQuery[K]] = key.map { k => new KeyBasedQuery[K](k, reftable, result, space, version, qid) }
}
