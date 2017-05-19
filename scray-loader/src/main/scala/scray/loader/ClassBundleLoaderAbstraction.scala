package scray.loader

import org.osgi.framework.BundleContext

/**
 * Abstraction layer to load classes or bundles
 */
trait ClassBundleLoaderAbstraction {
  
  /**
   * load either a class or bundle, depending on the String
   * must either be a classname, if this is loaded in a non-OSGI environment
   * or a bundle name, if it is loaded in a OSGI environment
   */
  def loadClassOrBundle[T](classOrBundle: String, className: String, interfaceClass: Class[T]): Option[T]
 
  
  /**
   * free resources
   */
  def stop(): Unit
}

object ClassBundleLoaderAbstraction {
  
  private var classbundleabstraction: Option[ClassBundleLoaderAbstraction] = None
  
  def setClassBundleLoaderAbstraction(bc: BundleContext, cba: ClassBundleLoaderAbstraction): Unit = {
    classbundleabstraction = Some(cba)
  }
  
  def getClassBundleLoaderAbstraction(): ClassBundleLoaderAbstraction = classbundleabstraction.get
}