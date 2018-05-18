package scray.jdbc.osgi

import org.osgi.framework.BundleContext
import org.osgi.framework.BundleActivator
import java.util.Hashtable

class Activator extends BundleActivator {
  
   override def start(context: BundleContext): Unit = {
     val fac = new ServiceFactory
     println(s"Register service with name ${classOf[InstanceFactory].getName} ")
     context.registerService(classOf[InstanceFactory].getName, fac, new Hashtable[String, String]())
   }
   
    override def stop(context: BundleContext): Unit = {
      
    }
  
}