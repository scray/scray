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
  override def getProperty(name: String): String = properties.get(name).orNull
  override def getBundle(): Bundle = ???
  override def installBundle(bundle: String): Bundle = ???
  override def installBundle(location: String, input: InputStream): Bundle = ???
	override def getBundle(id: Long): Bundle = ???
	override def getBundles(): Array[Bundle] = ???
	override def addServiceListener(listener: ServiceListener, filter: String): Unit = ???
	override def addServiceListener(listener: ServiceListener): Unit = ???
	override def removeServiceListener(listener: ServiceListener): Unit = ???
	override def addBundleListener(listener: BundleListener): Unit = ???
	override def removeBundleListener(listener: BundleListener): Unit = ???
	override def addFrameworkListener(listener: FrameworkListener): Unit = ???
	override def removeFrameworkListener(listener: FrameworkListener): Unit = ???
	override def registerService(clazzes: Array[String],
			service: Any, properties: Dictionary[_, _]): ServiceRegistration = ???
  override def registerService(clazz: String,
			service: Any, properties: Dictionary[_, _]): ServiceRegistration = ??? 
	override def getServiceReferences(clazz: String, filter: String): Array[ServiceReference] = ???
	override def getAllServiceReferences(clazz: String, filter: String): Array[ServiceReference] = ???
	override def getServiceReference(clazz: String): ServiceReference = ???
	override def getService(reference: ServiceReference): Object = ???
	override def ungetService(reference: ServiceReference): Boolean = ???
	override def getDataFile(filename: String): File = ???
	override def createFilter(filter: String): Filter = ???
}
