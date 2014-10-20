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

import scray.querying.description.Column
import scray.querying.description.EmptyRow
import scray.querying.description.Row
import scray.querying.description.internal.Domain
import scray.querying.description.internal.RangeValueDomain
import scray.querying.description.internal.SingleValueDomain
import scray.querying.queries.DomainQuery

/**
 * used to filter rows according to the domain parameters supplied
 * TODO: exclude filters which are inherent due to 
 */
class LazyQueryDomainFilterSource[Q <: DomainQuery](source: LazySource[Q]) 
  extends LazyQueryMappingSource[Q](source) {

  override def transformSpoolElement(element: Row, query: Q): Row = {
    // if we find a domain which is not matched by this Row we throw it away
    query.getWhereAST.find { domain =>
      element.getColumnValue[Any](domain.column) match {
        case None => true
        case Some(value) => domain match {
          case single: SingleValueDomain[Any] => !single.equiv.equiv(value, single.value)
          case range: RangeValueDomain[Any] => !range.valueIsInBounds(value)
        }
      }
    } match {
      case None => element
      case Some(x) => new EmptyRow
    }
  }
  
  /**
   * LazyQueryDomainFilterSource doesn't throw away columns (only rows), 
   * so we report back all columns from upstream
   */
  override def getColumns: List[Column] = source.getColumns
}


/**
 * used to filter rows according to the domain parameters supplied
 */
class EagerCollectingDomainFilterSource[Q <: DomainQuery, R](source: Source[Q, R]) 
  extends EagerCollectingQueryMappingSource[Q, R](source) {

  override def transformSeq(element: Seq[Row], query: Q): Seq[Row] = {
    element.filter { row => 
      query.getWhereAST.find { domain =>
        row.getColumnValue[Any](domain.column) match {
          case None => true
          case Some(value) => domain match {
            case single: SingleValueDomain[Any] => !single.equiv.equiv(value, single.value)
            case range: RangeValueDomain[Any] => !range.valueIsInBounds(value)
          }
        }
      } match {
        case None => true
        case Some(x) => false
      }
    }
  }

  override def transformSeqElement(element: Row, query: Q): Row = element

  override def getColumns: List[Column] = source.getColumns
}
