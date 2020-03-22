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

package scray.querying.sync


import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import scray.querying.sync.SyncTableBasicClasses.SyncTableRowEmpty
import scalaz.Plus
import java.util.Calendar


@RunWith(classOf[JUnitRunner])
class MergeTests extends WordSpec {
  
  "Merge-Api " should {
    " sum two values" in {
      
      val liveStream = ("A", 2) :: ("B", 3) ::  Nil
      val batchResult = (key: String) => {
         val values = Map("A" -> 42, "B" -> 128)
         values.get(key).get
      }
      
      val plus = (a: Int, b: Int) => a + b  
      
      assert(liveStream.map(Merge.merge[String, Int](_, plus, batchResult)) === List(44, 131))
    }
    " filter dates " in {
      
      // Filter all data which were produced after 1473866500
      
      val now = Calendar.getInstance.getTime
      now.setTime(1473866500000L)
      println(now)
      
      val past = Calendar.getInstance.getTime
      past.setTime(1473866200000L)
      println(past)
      
      val future = Calendar.getInstance.getTime
      future.setTime(1473866700000L)
      println(future)
      
      val timeFilter = new TimeFilter(now) 
      val imputStream =  (past, "dataA") :: (future, "dataB") :: Nil
 
      val results = imputStream.map(values => timeFilter.filter(values._1, values)).flatten
      
      assert(results.size == 1)
      assert(results.contains((past, "dataA")))
    }
   }
}
