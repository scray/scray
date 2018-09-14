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

package scray.hdfs.io.osgi

import org.osgi.framework.BundleActivator
import org.osgi.framework.BundleContext
import scray.hdfs.io.index.format.sequence.BinarySequenceFileWriter
import java.util.Hashtable
import scray.hdfs.io.write.WriteService

class Activator extends BundleActivator {

  override def start(context: BundleContext): Unit = {
    val fac = new ServiceFactory
    println(s"Register service with name ${classOf[WriteService].getName} ")
    context.registerService(classOf[WriteService].getName, fac, new Hashtable[String, String]())
  }

  override def stop(context: BundleContext): Unit = {

  }

}