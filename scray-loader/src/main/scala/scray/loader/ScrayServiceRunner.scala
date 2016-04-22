package scray.loader

import scray.querying.planning.PostPlanningActions
import scray.sil.properties.ScraySilPropertiesRegistration
import scray.core.service.ScrayCombinedStatefulTServer
import scray.core.service.ScrayCombinedStatefulTServer
import scray.querying.Registry
import scray.core.service.ScrayCombinedStatefulTServer

/**
 * Main server component for SIL-Scray 
 * 
 * Combined version with query ops and meta ops in a single service.
 *
 * In this project we intentionally do not provide any property files
 * in order to make sure we write one for the customer. So don't expect
 * this to run without
 */
object ScrayServiceRunner extends ScrayCombinedStatefulTServer {
  override def configureProperties: Unit = 
    ScraySilPropertiesRegistration.configure()
  
  lazy val space = SILQuerySpaceRegistrar()
  Registry.queryPostProcessor = PostPlanningActions.doNothing

  override def initializeResources: Unit = space
  override def destroyResources: Unit = {}
}
