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
package scray.loader.osgi

import org.osgi.framework.BundleContext
import org.osgi.framework.Bundle
import java.io.InputStream
import org.osgi.framework.FrameworkListener
import org.osgi.framework.BundleListener
import org.osgi.framework.ServiceListener
import java.util.Dictionary
import org.osgi.framework.ServiceRegistration
import org.osgi.framework.ServiceReference
import org.osgi.framework.Filter
import java.io.File

/**
 * this class fakes a BundleContext in order to run Scray without
 * any OSGI container, but still conform to the OSGI way of doing things.
 */
class FakeBundleContext(properties: Map[String, String]) extends BundleContext {
  def addBundleListener(x$1: org.osgi.framework.BundleListener): Unit = ???
  def addFrameworkListener(x$1: org.osgi.framework.FrameworkListener): Unit = ???
  def addServiceListener(x$1: org.osgi.framework.ServiceListener): Unit = ???
  def addServiceListener(x$1: org.osgi.framework.ServiceListener,x$2: String): Unit = ???
  def createFilter(x$1: String): org.osgi.framework.Filter = ???
  def getAllServiceReferences(x$1: String,x$2: String): 
   Array[org.osgi.framework.ServiceReference[_]] = ???
  def getBundle(x$1: String): org.osgi.framework.Bundle = ???
  def getBundle(x$1: Long): org.osgi.framework.Bundle = ???
  def getBundle(): org.osgi.framework.Bundle = ???
  def getBundles(): Array[org.osgi.framework.Bundle] = ???
  def getDataFile(x$1: String): java.io.File = ???
  def getProperty(x$1: String): String = ???
  def getService[S](x$1: org.osgi.framework.ServiceReference[S]): S = ???
  def getServiceReference[S](x$1: Class[S]): org.osgi.framework.ServiceReference[S] 
   = ???
  def getServiceReference(x$1: String): org.osgi.framework.ServiceReference[_] = ???
  def getServiceReferences[S](x$1: Class[S],x$2: String): 
   java.util.Collection[org.osgi.framework.ServiceReference[S]] = ???
  def getServiceReferences(x$1: String,x$2: String): 
   Array[org.osgi.framework.ServiceReference[_]] = ???
  def installBundle(x$1: String): org.osgi.framework.Bundle = ???
  def installBundle(x$1: String,x$2: java.io.InputStream): org.osgi.framework.Bundle 
   = ???
  def registerService[S](x$1: Class[S],x$2: S,x$3: java.util.Dictionary[String, _]): 
   org.osgi.framework.ServiceRegistration[S] = ???
  def registerService(x$1: String,x$2: Any,x$3: java.util.Dictionary[String, _]): 
   org.osgi.framework.ServiceRegistration[_] = ???
  def registerService(x$1: Array[String],x$2: Any,x$3: java.util.Dictionary[String, _]): 
   org.osgi.framework.ServiceRegistration[_] = ???
  def removeBundleListener(x$1: org.osgi.framework.BundleListener): Unit = ???
  def removeFrameworkListener(x$1: org.osgi.framework.FrameworkListener): Unit = ???
  def removeServiceListener(x$1: org.osgi.framework.ServiceListener): Unit = ???
  def ungetService(x$1: org.osgi.framework.ServiceReference[_]): Boolean = ???
}
