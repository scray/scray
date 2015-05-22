package scray.core.service

import scray.core.service.properties.ScrayServicePropertiesRegistration

object ScrayTTestServer extends ScrayStatefulTServer {
  // initialize properties
  override def configureProperties: Unit = ScrayServicePropertiesRegistration.configure()

  def initializeResources: Unit = {}
  def destroyResources: Unit = {}
}
