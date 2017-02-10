//package scray.loader.integrationtests
//
//import com.typesafe.scalalogging.slf4j.LazyLogging
//
//import java.net.Socket
//
//import org.junit.runner.RunWith
//import org.scalatest.WordSpec
//import org.scalatest.junit.JUnitRunner
//
//import scray.loader.osgi.{ Activator, FakeBundleContext }
//import scray.querying.Registry
//import scray.querying.queries.SimpleQuery
//import scray.querying.description.internal.RangeValueDomain
//import scray.querying.description.TableIdentifier
//import scray.loader.ScrayStandaloneService
//import scray.querying.planning.Planner
//import scray.common.properties.ScrayProperties
//import scray.querying.description.Column
//import scray.querying.description.internal.Bound
//import scray.querying.description.GreaterEqual
//import com.twitter.util.Await
//import scray.querying.description.Equal
//
//@RunWith(classOf[JUnitRunner])
//class ScrayIntegrationTests extends WordSpec with LazyLogging {
//  
//  "Scray-Loader Activator" should {
//    
//    def addSomeData {
//      // CREATE TABLE "SCHWAETZ" (key text, data text, lucene text, time bigint, PRIMARY KEY (key))
//      /* CREATE CUSTOM INDEX time_index ON "SCHWAETZ" (lucene)
//       * USING 'com.stratio.cassandra.lucene.Index'
//       * WITH OPTIONS = {
//       *   'refresh_seconds' : '1',
//       *   'schema' : '{
//       *      fields : {
//       *        time  : {type : "long", sorted : true}
//       *      }
//       *   }'
//       * };
//       * create index dat_index on "SCHWAETZ" (data);
//       */
//      // insert into "SCHWAETZ" (key, data, time) VALUES ('1', '1', 12345);
//      // insert into "SCHWAETZ" (key, data, time) VALUES ('2', '2', 54321);
//      // insert into "SCHWAETZ" (key, data, time) VALUES ('3', '2', 54321);
//      // insert into "SCHWAETZ" (key, data, time) VALUES ('4', '2', 44444);
//      // insert into "SCHWAETZ" (key, data, time) VALUES ('5', '2', 86786);
//      // insert into "SCHWAETZ" (key, data, time) VALUES ('6', '2', 55555);
//    }

//    
////    "be able to start up a in-process scray interface and make a simple query" in {
////      waitForPortClosed
////      // this demonstrates how to run Scray from within a simple Java/Scala program 
////      val activator = new Activator
////      val context = new FakeBundleContext(Map(Activator.OSGI_FILENAME_PROPERTY -> "resource:///integrationtestconfigs/mainconfig1.txt"))
////      activator.start(context)
////      printQuerySpace
////      val query = SimpleQuery("HelloWorld", TableIdentifier( "test", "BLA", "SCHWAETZ" ))
////      logger.info("------------------------------------------------Bluequery is:" + query)
////      
////      Planner.planAndExecute(query)
////      
////      Activator.keepRunning = false
////    }
//
//    "be able to start up a in-process scray interface and make a Cassandra secondary-index query" in {
//      waitForPortClosed
//      ScrayProperties.reset()
//      // this demonstrates how to run Scray from within a simple Java/Scala program 
//      val activator = new Activator
//      val context = new FakeBundleContext(Map(Activator.OSGI_FILENAME_PROPERTY -> "resource:///integrationtestconfigs/mainconfig1.txt"))
//      activator.start(context)
//      printQuerySpace
//      val ti = TableIdentifier( "test", "BLA", "SCHWAETZ" )
//      val tcol = Column("data", ti)
//      val query = SimpleQuery("HelloWorld", ti, where = Some(Equal(tcol, "2")))
//      logger.info("query is:" + query)
//      var result = Planner.planAndExecute(query)
//      while(!result.isEmpty) {
//        logger.info(s"Got result: ${result.head.getColumnValue(tcol)}")
//        result = Await.result(result.tail)
//      }
//      Activator.keepRunning = false
//    }
//
////    "be able to start up a in-process scray interface and make a lucene query" in {
////      waitForPortClosed
////      ScrayProperties.reset()
////      // this demonstrates how to run Scray from within a simple Java/Scala program 
////      val activator = new Activator
////      val context = new FakeBundleContext(Map(Activator.OSGI_FILENAME_PROPERTY -> "resource:///integrationtestconfigs/mainconfig1.txt"))
////      activator.start(context)
////      printQuerySpace
////      val ti = TableIdentifier( "test", "BLA", "SCHWAETZ" )
////      val tcol = Column("time", ti)
////      val query = SimpleQuery("HelloWorld", ti, where = Some(GreaterEqual(tcol, 54000L)))
////      logger.info("query is:" + query)
////      var result = Planner.planAndExecute(query)
////      while(!result.isEmpty) {
////        logger.info(s"Got result: ${result.head.getColumnValue(tcol)}")
////        result = Await.result(result.tail)
////      }
////      Activator.keepRunning = false
////    }
//
//    "be able to start a stand-alone scray query service" in {
//      waitForPortClosed
//      ScrayProperties.reset()
//      ScrayStandaloneService.main(Array("-c", "resource:///integrationtestconfigs/mainconfig1.txt"))
//    }
//
//    "query the stand-alone scray query service" in {
//      waitForPortClosed
//      ScrayProperties.reset()
//      ScrayStandaloneService.main(Array("-c", "resource:///integrationtestconfigs/mainconfig1.txt", "--stateless"))
//      // o.k. service should be running, make a query
//      
//    }
//  }
//  
//  def printQuerySpace = {
//    val names = Registry.getQuerySpaceNames()
//    println("Printing queryspaces..." + names)    
//    names.foreach { x =>
//      println("found tables for " + x + " : " + Registry.getQuerySpaceTables(x, 0))
//    }
//  }
//  
//  def waitForPortClosed = {
//    var shutdown = false
//    while(!shutdown) {
//      try {
//        val echoSocket = new Socket("localhost", 18181);
//        echoSocket.getOutputStream
//        shutdown = false
//        echoSocket.close()
//      } catch {
//        case e: Exception => shutdown = true 
//      } 
//      Thread.sleep(2000)
//    }
//  }
//}