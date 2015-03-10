
package scray.core.service.properties

import scray.common.properties.ScrayProperties
import scray.common.properties.predefined.PredefinedProperties
import scray.common.properties.ScrayPropertyRegistration
import scray.common.properties.PropertyFileStorage

object ScrayServicePropertiesRegistrar extends ScrayPropertyRegistration.PropertyRegistrar {
  
  override def register() : Unit = {
    ScrayProperties.registerProperty(PredefinedProperties.SCRAY_SERVICE_IPS)
    ScrayProperties.registerProperty(PredefinedProperties.SCRAY_MEMCACHED_IPS)
    ScrayProperties.registerProperty(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE)
  }
}

object ScrayServicePropertiesLoader extends ScrayPropertyRegistration.PropertyLoaderImpl("scray-service-loader") {
  def load() : Unit = {
    ScrayProperties.addPropertiesStore(new PropertyFileStorage(
      "scray-service.properties", PropertyFileStorage.FileLocationTypes.JarPropertiesFile))
    ScrayProperties.addPropertiesStore(new PropertyFileStorage(
      "scray_service_properties", PropertyFileStorage.FileLocationTypes.LocalPropertiesFile))
  }
}
