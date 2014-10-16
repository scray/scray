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
package scray.querying.source

import com.twitter.util.Future
import com.twitter.concurrent.Spool
import scray.querying.description.{Column, Row}
import scray.querying.queries.DomainQuery

/**
 * sources take queries and produce future results.
 * All queryable components in scray should be Sources.
 */
trait Source[Q <: DomainQuery, T] {

  def request(query: Q): Future[T]

  /**
   * the result will be comprised of a set of columns
   */
  def getColumns: List[Column]
  
  /**
   * whether this source will return elements in the order
   * as defined in this DomainQuery
   */
  def isOrdered(query: Q): Boolean
}

/**
 * a lazy Source is a component one can issue queries upon and get back
 * a Spool which contains the resulting data of type R. This should be
 * used as often as possible to prevent pulling all data into memory.
 */
trait LazySource[Q <: DomainQuery] extends Source[Q, Spool[Row]] {
  override def request(query: Q): LazyData
}

/**
 * an eager Source is a component one can issue queries upon and get back
 * a Collection which contains the resulting data of type R. Everything gets
 * pulled into memory at once.
 */
trait EagerSource[Q <: DomainQuery] extends Source[Q, Seq[Row]] {
  override def request(query: Q): EagerData
}

/**
 * A source that returns nothing.
 */
class NullSource[Q <: DomainQuery] extends LazySource[Q] {
  override def request(query: Q): LazyData = Future(Spool.Empty)
  override def getColumns: List[Column] = List()
  override def isOrdered(query: Q): Boolean = true
}
