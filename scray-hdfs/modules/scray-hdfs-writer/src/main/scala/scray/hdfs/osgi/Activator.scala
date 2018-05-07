package scray.hdfs.osgi

import org.osgi.framework.BundleActivator
import org.osgi.framework.BundleContext
import scray.hdfs.index.format.sequence.BinarySequenceFileWriter
import java.util.Hashtable

class Activator extends BundleActivator {
  
   override def start(context: BundleContext): Unit = {
     val fac = new ServiceFactory
     println(s"Register service with name ${classOf[BinarySequenceFileWriter].getName} ")
     context.registerService(classOf[BinarySequenceFileWriter].getName, fac, new Hashtable[String, String]())
   }
   
    override def stop(context: BundleContext): Unit = {
      
    }
  
}