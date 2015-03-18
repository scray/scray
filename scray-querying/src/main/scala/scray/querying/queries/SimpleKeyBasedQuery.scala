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
 * Query to look up a list of columns (usually a single one) primary key in a table.
 * In case columns has more than one entry K is a tuple.
 */
class SimpleKeyBasedQuery[K](override val key: K, columns: List[Column], result: List[Column], space: String, qid: UUID)
  extends KeyBasedQuery[K](key, columns(0).table, result, space, qid) {
  
  override def transformedAstCopy(ast: List[Domain[_]]): SimpleKeyBasedQuery[K] = if(ast.size != columns.size) {
    throw new KeyBasedQueryException(this)
  } else {
    if(ast.size == 1) {
      ast(0) match {
        case svd: SingleValueDomain[K] => new SimpleKeyBasedQuery[K](svd.value, List(svd.column), result, space, qid)
        case _ => throw new KeyBasedQueryException(this)
      }
    } else {
      val cols = ast.map { _ match {
          case svd: SingleValueDomain[_] => svd.column
          case _ => throw new KeyBasedQueryException(this)
        }
      }
      val key = createTuple(ast.map { _ match {
          case svd: SingleValueDomain[_] => svd.value.asInstanceOf[AnyRef]
          case _ => throw new KeyBasedQueryException(this)        
        }
      })
      new SimpleKeyBasedQuery[K](key, cols, result, space, qid)
    }
  }
  
  private def createTuple[A <: AnyRef, K](list: List[A]): K =
    Class.forName(s"scala.Tuple${list.size}").getConstructors.apply(0).newInstance(list:_*).asInstanceOf[K]
}
