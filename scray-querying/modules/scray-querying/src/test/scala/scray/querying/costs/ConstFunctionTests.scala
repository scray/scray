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
package scray.querying.costs

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.description.SimpleRow
import scray.querying.description.TableIdentifier
import scray.querying.description.Column
import scray.querying.description.RowColumn
import com.twitter.util.Future
import scray.querying.description.Row
import scray.querying.description.ColumnOrdering
import com.twitter.util.Await
import com.twitter.concurrent.Spool
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scray.querying.source.costs.LinearQueryCostFuntionFactory


@RunWith(classOf[JUnitRunner])
class ConstFunctionTests extends WordSpec {
  "Const functions " should {
    "use the factory " in {
      
      val factory = LinearQueryCostFuntionFactory
      
      ///val costfunction = factory.apply(null)
      
      
      
      assert(true)
      
    }
  }
  
}