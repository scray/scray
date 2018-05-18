package scray.jdbc.osgi

import org.osgi.framework.ServiceRegistration
import scray.jdbc.sync.JDBCDbSession
import org.osgi.framework.Bundle

class ServiceFactory extends org.osgi.framework.ServiceFactory[InstanceFactory] {
  
  def getService(bundle: Bundle, reg: ServiceRegistration[InstanceFactory]): InstanceFactory = {
    new InstanceFactory
  }
  
  def ungetService(bundle: Bundle, reg: ServiceRegistration[InstanceFactory], writer: InstanceFactory): Unit = {
    
  }
}