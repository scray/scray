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
package scray.querying.planning

import scray.querying.queries.DomainQuery
import org.apache.commons.io.FileUtils

/**
 * things we can do with dot graph information from the planner
 */
object PostPlanningActions {

  type PostPlanningAction = (DomainQuery, ComposablePlan[_, _]) => Unit
  
  /**
   * self-explaining :)
   */
  val doNothing: PostPlanningAction = (query: DomainQuery, plan: ComposablePlan[_, _]) => ()
  
  /**
   * write out the plan as a dot file into tmp.dir
   */
  val writeDot: PostPlanningAction = (query: DomainQuery, plan: ComposablePlan[_, _]) => {
    val data = plan.getDot(query.getQueryID.toString())
    FileUtils.write(FileUtils.getFile(FileUtils.getTempDirectory(), 
        s"scray${query.getQueryID.toString()}.dot"), data, false)  
  }
  
}