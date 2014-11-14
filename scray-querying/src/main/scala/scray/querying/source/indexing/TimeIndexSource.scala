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
package scray.querying.source.indexing

import scray.querying.source.LazySource
import scray.querying.queries.DomainQuery
import scray.querying.source.AbstractHashJoinSource
import scray.querying.description.TableIdentifier
import scray.querying.source.KeyValueSource
import scray.querying.description.Column
import scray.querying.description.Row
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/**
 * creates an indexed-source with a hashed-join reference on a time column.
 * 
 * To achieve query transformation
 */
class TimeIndexSource[Q <: DomainQuery, R <: Product, V](
    timeIndexConfig: TimeIndexConfig,
    indexsource: LazySource[Q],
    lookupSource: KeyValueSource[R, V],
    lookupSourceTable: TableIdentifier)(implicit tag: ClassTag[R]) 
    extends AbstractHashJoinSource[Q, R, V](indexsource, lookupSource, lookupSourceTable) {

  override protected def transformIndexQuery(query: Q): Q = {
    query
  }
  
  protected def getJoinablesFromIndexSource(index: Row): Array[R] = {
    index.getColumnValue(timeIndexConfig.indexReferencesColumn) match {
      case Some(refs) => refs match {
        case travs: TraversableOnce[R] => travs.asInstanceOf[TraversableOnce[R]].toArray
        case travs: R => Array[R](travs)
      }
      case None => Array[R]()
    }
  }

  /**
   * since this is a true index only  
   */
  def getColumns: List[Column] = lookupSource.getColumns
}

case class TimeIndexConfig(timeReferenceCol: Column, indexReferencesColumn: Column) extends IndexConfig
