package scray.loader.osgi

import org.osgi.framework.BundleContext
import scray.loader.ClassBundleLoaderAbstraction
import scray.common.errorhandling.ErrorHandler
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import java.net.URLClassLoader
import java.net.URL
import scray.common.errorhandling.ScrayProcessableErrors
import scray.common.errorhandling.LoadCycle

/**
 * Loads plugin classes in Scray using a simple reflection call.
 * 
 * important: classes handled by this interface may never have contructor parameters! 
 */
class ScraySimpleClassloaderPluginFacility (errorHandler: ErrorHandler) extends ClassBundleLoaderAbstraction {
  
  val lock = new ReentrantLock
  val loadedClasses = new HashMap[String, Any]()
  
  private def instantiateClass[T](jarFile: String, className: String, interfaceClass: Class[T]): T = {
    try {
      if(jarFile.trim == "") {
        Class.forName(className).newInstance().asInstanceOf[T]
      } else {
        val url = URLClassLoader.newInstance(Array[URL](new URL(jarFile)))
        val clazz = url.loadClass(className)
        clazz.newInstance().asInstanceOf[T]
      }
    } catch {
      case _: Exception => null.asInstanceOf[T]
    }
  }
  
  /**
   * performs classloading actions.
   * if jarFile is the empty String use current classloader, else instantiate URLClassLoader and load class from
   * provided jar given as an URL-String
   */
  override def loadClassOrBundle[T](jarFile: String, className: String, interfaceClass: Class[T]): Option[T] = {
    lock.lock()
    try {
      // fetch the instance or create new one
      loadedClasses.get(className).map { _.asInstanceOf[T] }.orElse {
        // use reflection to construct class
        val obj = Option(instantiateClass(jarFile, className, interfaceClass))
        obj.map { o =>
          loadedClasses += ((className, o))
          o
        }.orElse {
          errorHandler.handleError(ScrayProcessableErrors.CLASSLOADER_LOAD_FAILED, LoadCycle.RUNTIME,
              s"Could not load class ${className} from JAR ${jarFile}.")
          None
        }
      }
    } finally {
      lock.unlock()
    } 
  }
  
  /**
   * cannot unload classes from classloaders
   */
  override def stop(): Unit = {}
  
}