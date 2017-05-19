package scray.loader.osgi

import scray.loader.ClassBundleLoaderAbstraction
import org.osgi.framework.BundleContext
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable.ArrayBuffer
import org.osgi.util.tracker.ServiceTracker
import scray.common.errorhandling.ErrorHandler
import scray.common.errorhandling.ScrayProcessableErrors
import scray.common.errorhandling.LoadCycle
import scala.util.Try

/**
 * Handles plugins for Scray using OSGI
 */
class ScrayOSGIBundlePluginFacility(bundleContext: BundleContext, errorHandler: ErrorHandler) extends ClassBundleLoaderAbstraction {
  
  private val lock = new ReentrantLock
  private val services = new ArrayBuffer[ServiceTracker[_, _]]()
  
  /**
   * installs and starts bundle classOrBundle and then tracks and open service interfaceClass 
   */
  def loadClassOrBundle[T](classOrBundle: String, className: String, interfaceClass: Class[T]): Option[T] = {
    // install and start bundle
    Try {
      val bundle = bundleContext.installBundle(classOrBundle)
      bundle.start()
    }.map { _ =>
      
      // track service
      val serviceTracker = new ServiceTracker[T, T](bundleContext, className, null)
      serviceTracker.open()
      
      val service = Option(serviceTracker.getService)
      service.map { s => 
        lock.lock()
        try {
          services += serviceTracker
          s
        } finally {
          lock.unlock()
        }
      }.orElse {
        errorHandler.handleError(ScrayProcessableErrors.OSGI_SERVICE_FAILED, LoadCycle.RUNTIME, 
            s"Opening OSGI Service class ${className} failed")
        None
      }
    }.getOrElse {
      errorHandler.handleError(ScrayProcessableErrors.OSGI_BUNDLE_FAILED, LoadCycle.RUNTIME,
          s"Starting OSGI Bundle ${classOrBundle} failed")
      None
    }
  }
 
  /**
   * stops all services previously opened
   */
  def stop(): Unit = {
    lock.lock()
    try {
      services.foreach { serviceTracker =>
        serviceTracker.close()
      }
    } finally {
      lock.unlock()
    }
  }
}