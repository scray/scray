package scray.loader.configuration

import scray.common.properties.ScrayPropertyRegistration
import scray.common.properties.ScrayProperties
import scray.common.properties.predefined.PredefinedProperties

object LoaderProperties extends ScrayPropertyRegistration.PropertyRegistrar {
  override def register(): Unit = {
    ScrayProperties.registerProperty(PredefinedProperties.QUERY_READ_CONSISTENCY)
  }
  
}