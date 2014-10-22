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
package scray.querying

import com.twitter.util.Future
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import scray.querying.description.Column

package object source {
  type LazyData = Future[Spool[Row]]
  type EagerData = Future[Seq[Row]]
  
  /**
   * compares two rows according to a column
   * empty rows or rows without column in question are "bigger" than the others 
   */
  def rowCompWithOrdering[T](column: Column, ordering: Ordering[T]): (Row, Row) => Boolean = (row1, row2) => {
    val a = row1.getColumnValue(column)
    val b = row2.getColumnValue(column)
    if(a.isEmpty) { 
      if(b.isEmpty) {
        true
      } else {
        false
      }
    } else { 
      if(b.isDefined) { 
        ordering.compare(a.get, b.get) <= 0
      } else { 
        true 
      }
    }
  } 
}
