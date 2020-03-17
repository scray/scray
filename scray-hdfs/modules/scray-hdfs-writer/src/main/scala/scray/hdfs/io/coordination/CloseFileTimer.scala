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

package scray.hdfs.io.coordination

import java.util.TimerTask

import com.typesafe.scalalogging.LazyLogging

class CloseFileTimer(writer: CoordinatedWriter[_, _, _, _], val timerDelay: Long) extends TimerTask with LazyLogging {
  
  private val startTime = System.currentTimeMillis()
  
  /**
   * Estimated remaining time
   */
  def getRemainingTime: Long = {
    (startTime + timerDelay) - System.currentTimeMillis()
  }
  
  def run() = {
    logger.debug(s"Try to close writer ${writer.getPath}")
    writer.close
    logger.debug("Writer closed")
  }
  
}