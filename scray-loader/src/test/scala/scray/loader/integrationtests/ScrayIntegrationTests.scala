package scray.loader.integrationtests

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.junit.runner.RunWith
import org.scalatest.{ Finders, WordSpec }
import org.scalatest.junit.JUnitRunner
import scray.loader.osgi.Activator
import scray.loader.osgi.FakeBundleContext

@RunWith(classOf[JUnitRunner])
class ScrayIntegrationTests extends WordSpec with LazyLogging {
  "Scray-Loader Activator" should {
    "be able to start up a scray service" in {
      // this demonstrates how to run Scray from within a simple Java/Scala program 
      val activator = new Activator
      val context = new FakeBundleContext(Map(Activator.OSGI_FILENAME_PROPERTY -> "resource:///integrationtestconfigs/mainconfig1.txt"))
      activator.start(context)
      Activator.keepRunning = false
    }
  }
}