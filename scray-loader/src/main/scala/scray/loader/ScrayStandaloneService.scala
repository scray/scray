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
package scray.loader

import com.typesafe.scalalogging.LazyLogging

import scray.loader.osgi.Activator
import scray.loader.osgi.FakeBundleContext


/**
 * runs the scray service as a stand-alone application
 */
object ScrayStandaloneService extends LazyLogging {
  
  /**
   * Main config filename and if we want a stateful or a stateless service
   */
  case class ScrayProgramArguments(mainConfigFile: String = "", statelessService: Boolean = false, poolSize: Int = DEFAULT_THREAD_POOL_SIZE)
  
  /**
   * Parser to parse command line arguments
   */
  val parser = new scopt.OptionParser[ScrayProgramArguments]("ScrayStandaloneService") {
    // need a URL for the main config file 
    // some help information
    head("ScrayStandaloneService", VERSION)
    opt[String]('c', "config").required().action((x, c) =>
      c.copy(mainConfigFile = x)).text("URL to the main config file to be loaded on startup. Supports resource- and hdfs-URL-schemes to provide class-path or hdfs-based locations. Required.")
    opt[Unit]("stateless").action((_, c) =>
      c.copy(statelessService = true)).text("Start as a stateless service. This requires Memcache to be up and running. Not recommended. Default is stateful.")
    opt[Int]('t', "threads").action((x, c) =>
      c.copy(poolSize = x)).text("Scray uses a thread pool to organize store requests; the number of threads can be controlled with this argument. Default is " + DEFAULT_THREAD_POOL_SIZE)
    help("help").text("prints this usage text")
  }
  
  /**
   * main: start fake bundle context to simulate OSGI env
   * for config you can provide s.th. like "resource:///integrationtestconfigs/mainconfig1.txt"
   */
  def main(args: Array[String]) = {
    val config = parser.parse(args.toSeq, ScrayProgramArguments())
    if(config.isDefined) {
      val activator = new Activator
      val context = new FakeBundleContext(Map(Activator.OSGI_FILENAME_PROPERTY -> config.get.mainConfigFile, Activator.POOLSIZE_PROPERTY -> config.get.poolSize.toString))
      logger.info(s"Starting Standalone-Scray-Service version $VERSION")
      activator.start(context)
    } else {
      // failed to parse, so print usage information
      parser.showTryHelp
    }
  }
}
