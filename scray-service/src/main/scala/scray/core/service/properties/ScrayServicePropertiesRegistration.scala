package scray.core.service.properties

import scray.common.properties.ScrayPropertyRegistration
import scray.common.properties.predefined.CommonCassandraLoader
import scray.common.properties.predefined.CommonCassandraRegistrar

object ScrayServicePropertiesRegistration {

  def configure() : Unit = {
    ScrayPropertyRegistration.addRegistrar(ScrayServicePropertiesRegistrar)
    ScrayPropertyRegistration.addLoader(ScrayServicePropertiesLoader)
    ScrayPropertyRegistration.performPropertySetup()
  }

}