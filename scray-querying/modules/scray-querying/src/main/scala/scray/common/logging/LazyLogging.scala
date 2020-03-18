//package scray.common.logging
//
//import com.typesafe.scalalogging.slf4j.{LazyLogging => SLazyLogging}
//import com.typesafe.scalalogging.Logger
//import org.slf4j.LoggerFactory
//import org.slf4j.{Logger => JLogger} 
//import java.lang.reflect.Constructor
//
//trait LazyLogging extends SLazyLogging {
//  
//    @transient override protected lazy val logger: Logger = {
//      val constructor = classOf[Logger].getConstructor(classOf[String])
//      constructor.setAccessible(true)
//      constructor.newInstance(LoggerFactory getLogger getClass.getName)
//    }
//}
//  
