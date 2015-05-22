package scray.core.service.properties

import scray.common.properties.{ ScrayProperties, ScrayPropertyRegistration, PropertyFileStorage }
import scray.common.properties.predefined.PredefinedProperties
import com.twitter.util.Duration
import java.util.concurrent.TimeUnit

object ScrayServicePropertiesRegistrar extends ScrayPropertyRegistration.PropertyRegistrar {

  val SCRAY_ENDPOINT_LIFETIME =
    new DurationProperty("SCRAY_ENDPOINT_LIFETIME", Duration(5, TimeUnit.MINUTES))

  override def register(): Unit = {
    ScrayProperties.registerProperty(PredefinedProperties.SCRAY_SERVICE_HOST)
    ScrayProperties.registerProperty(PredefinedProperties.SCRAY_QUERY_PORT)
    ScrayProperties.registerProperty(PredefinedProperties.SCRAY_META_PORT)
    ScrayProperties.registerProperty(PredefinedProperties.SCRAY_MEMCACHED_IPS)
    ScrayProperties.registerProperty(PredefinedProperties.RESULT_COMPRESSION_MIN_SIZE)
    ScrayProperties.registerProperty(PredefinedProperties.SCRAY_SEED_IPS)
    ScrayProperties.registerProperty(SCRAY_ENDPOINT_LIFETIME)
  }
}

object ScrayServicePropertiesLoader extends ScrayPropertyRegistration.PropertyLoaderImpl("scray-service-loader") {
  def load(): Unit = {
    ScrayProperties.addPropertiesStore(new PropertyFileStorage(
      "scray-service.properties", PropertyFileStorage.FileLocationTypes.JarPropertiesFile))
    ScrayProperties.addPropertiesStore(new PropertyFileStorage(
      "scray_service_properties", PropertyFileStorage.FileLocationTypes.LocalPropertiesFile))
  }
}
