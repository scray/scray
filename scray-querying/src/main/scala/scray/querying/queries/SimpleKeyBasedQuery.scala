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
 * query to look up a single column primary key in a table
 */
class SimpleKeyBasedQuery[K](override val key: K, column: Column, result: List[Column], space: String, qid: UUID)
  extends KeyBasedQuery[K](key, column.table, result, space, qid) {
  
  override def transformedAstCopy(ast: List[Domain[_]]): SimpleKeyBasedQuery[K] = ast.headOption.map {
    _ match {
      case svd: SingleValueDomain[K] => new SimpleKeyBasedQuery[K](svd.value, svd.column, result, space, qid)
      case _ => throw new KeyBasedQueryException(this)
    }
  }.orElse(throw new KeyBasedQueryException(this)).get
}
