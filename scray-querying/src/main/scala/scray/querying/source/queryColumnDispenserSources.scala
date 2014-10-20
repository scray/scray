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

import scray.querying.queries.DomainQuery
import scray.querying.description.{Column, Row, RowColumn, SimpleRow}

object ColumnDispenserTransformer {
  def transformElement[Q <: DomainQuery](element: Row, query: Q): Row = {
    // if we find a column which is not requested by this query we will dispense it
    val columns = element.getColumns.filter(query.getResultSetColumns.contains(_))
    SimpleRow(columns.map(col => RowColumn(col, element.getColumnValue(col).get)))
  }
}

/**
 * used to filter columns according to the query parameters supplied,
 * i.e. throw away columns which are not needed any more
 */
class LazyQueryColumnDispenserSource[Q <: DomainQuery](source: LazySource[Q]) 
  extends LazyQueryMappingSource[Q](source) {

  def transformSpoolElement(element: Row, query: Q): Row = 
    ColumnDispenserTransformer.transformElement(element, query)

  /**
   * This is the maximum we can return, if a query will request them all
   */
  override def getColumns: List[Column] = source.getColumns
}


/**
 * used to filter rows according to the domain parameters supplied
 */
class EagerCollectingColumnDispenserSource[Q <: DomainQuery, R](source: Source[Q, R]) 
  extends EagerCollectingQueryMappingSource[Q, R](source) {

  override def transformSeqElement(element: Row, query: Q): Row = 
    ColumnDispenserTransformer.transformElement(element, query)

  override def getColumns: List[Column] = source.getColumns
}
