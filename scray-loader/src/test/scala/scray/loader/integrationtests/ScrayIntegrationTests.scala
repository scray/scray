package scray.loader.integrationtests

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.junit.runner.RunWith
import org.scalatest.{ Finders, WordSpec }
import org.scalatest.junit.JUnitRunner
import scray.loader.osgi.Activator
import scray.loader.osgi.FakeBundleContext
import scray.querying.planning.Planner
import scray.querying.queries.SimpleQuery
import scray.querying.description.TableIdentifier
import scray.querying.Registry
import scray.loader.ScrayStandaloneService
import scray.common.properties.ScrayProperties
import java.net.Socket

@RunWith(classOf[JUnitRunner])
class ScrayIntegrationTests extends WordSpec with LazyLogging {
  
  "Scray-Loader Activator" should {
    
    "be able to start up a in-process scray interface" in {
      waitForPortClosed
      // this demonstrates how to run Scray from within a simple Java/Scala program 
      val activator = new Activator
      val context = new FakeBundleContext(Map(Activator.OSGI_FILENAME_PROPERTY -> "resource:///integrationtestconfigs/mainconfig1.txt"))
      activator.start(context)
      printQuerySpace
      val query = SimpleQuery("HelloWorld", TableIdentifier( "test", "BLA", "SCHWAETZ" ))
      
      Planner.planAndExecute(query)
      
      Activator.keepRunning = false
    }
    
    "be able to start a stand-alone scray query service" in {
      waitForPortClosed
      ScrayProperties.reset()
      ScrayStandaloneService.main(Array("-c", "resource:///integrationtestconfigs/mainconfig1.txt"))
    }

    "query the stand-alone scray query service" in {
      waitForPortClosed
      ScrayProperties.reset()
      ScrayStandaloneService.main(Array("-c", "resource:///integrationtestconfigs/mainconfig1.txt", "--stateless"))
      // o.k. service should be running, make a query
      
    }
  }
  
  def printQuerySpace = {
    val names = Registry.getQuerySpaceNames()
    println("Printing queryspaces..." + names)    
    names.foreach { x =>
      println("found tables for " + x + " : " + Registry.getQuerySpaceTables(x, 0))
    }
  }
  
  def waitForPortClosed = {
    var shutdown = false
    while(!shutdown) {
      try {
        val echoSocket = new Socket("localhost", 18181);
        echoSocket.getOutputStream
        shutdown = false
        echoSocket.close()
      } catch {
        case e: Exception => shutdown = true 
      } 
      Thread.sleep(2000)
    }
  }
}