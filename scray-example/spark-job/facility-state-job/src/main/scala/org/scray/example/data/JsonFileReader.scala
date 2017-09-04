package de.s_node.data.db

import scala.io.Source._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.twitter.concurrent.AsyncStream
import com.twitter.util.Future

import de.s_node.opendata.db.TimeDecorator
import java.nio.file.Paths
import akka.stream.scaladsl.FileIO
import akka.stream.scaladsl.Framing
import akka.util.ByteString
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Flow
import akka.stream.ActorMaterializer
import akka.actor.ActorSystem
import akka.stream._
import akka.stream._
import akka.stream.scaladsl._
import akka.NotUsed
import java.util.concurrent.LinkedBlockingQueue
import scray.example.db.fasta.model.Facility


class JsonFileReader {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def jsonReader(json: String): Option[Seq[Facility]] = {
    try {
      return Some(mapper.readValue(json, classOf[Array[Facility]]).toSeq)
    } catch {
      case e: Throwable => {
        println("Parserror in line " + json);
        return None
      }
      case _ => println("FFFFFFFFFFF")
    }
    return None
  }

  def timeParser(timeString: String): Option[Long] = {
    try {
      Some(timeString.toLong)
    } catch {
      case e: NumberFormatException => {
        println("Parserror in line" + timeString + "Should be a timestamp")
        None
      }
    }
  }

//  def redFromFile(path: String): AsyncStream[TimeDecorator[Facility]] = {
//    val lines = fromFile(path).getLines
//    def mkStream(): AsyncStream[String] = AsyncStream.fromFuture(Future(lines.next())) ++ mkStream()
//
//    val ddd = mkStream().map(parse).map(xxx => addTimestamp(xxx)).flatten
//    
//    ddd
//  }
  
    def redFromFile(path: String) = {
      val lines = fromFile(path).getLines
      def mkStream(): AsyncStream[String] = AsyncStream.fromFuture(Future(lines.next())) ++ mkStream()
  
      val ddd = mkStream().map(parse)
      
      ddd
  }
    
  def redFromFileFF(path: String) = {
      val lines = fromFile(path).getLines
      
      def getNext: Future[String] = {
        
        val ddd  = Future(System.currentTimeMillis().toString())
        
        //println(ddd.isDefined + "\t"  )
        ddd
        
      }
      
      def mkStream : AsyncStream[Future[String]] = getNext  +:: mkStream
      
      // def mkStream2 : AsyncStream[String] = ""  +:: mkStream
      
      
      mkStream
  }
  
  def akkaFileReader(path: String, queue: LinkedBlockingQueue[TimeDecorator[Facility]]) = {
    
     implicit val system = ActorSystem("reactive-tweets")
     implicit val materializer = ActorMaterializer()
    val line = FileIO.fromPath(Paths.get(path)).
      via(Framing.delimiter(ByteString(System.lineSeparator), 1000000, allowTruncation = true)).
      map(_.utf8String).
      map { line => parse(line) }.
      map { timeFacility => decorateTime(timeFacility)}.
      map { ff => ff.flatten}.
      map (dd => dd.getOrElse(Seq.empty[Seq[TimeDecorator[Facility]]])).
      map (fff => fff.toList).
      map(fff => {fff}).
      //map (dd => {dd.foreach { x => println(x.asInstanceOf[TimeDecorator[Facility]].getData.getEquipmentnumber)}; dd}).
      mapConcat(identity).
      runForeach ( x => {
        //println(x.asInstanceOf[TimeDecorator[Facility]].getData.getEquipmentnumber) 
        queue.put(x.asInstanceOf[TimeDecorator[Facility]])
      })(materializer)

      
    //val sink: Sink[String] = Sink.foreach(x => println(x.split(",").size))
    

    
  }

  def parse(line: String): Tuple2[Option[Long],Option[Seq[Facility]]] = {
    val timeArrayString = line.splitAt(10)
    (timeParser(timeArrayString._1), this.jsonReader(timeArrayString._2))
  }
  
  
  def decorateTime(facilityAndTime: (Option[Long], Option[Seq[Facility]])) = {
    facilityAndTime._1.map { time =>  
        facilityAndTime._2.map { facilities => {
          facilities.map { facility => {
              new TimeDecorator((time * 1000), facility)
            }
          }
        }
      }
    }
  }
  

  
  def addTimestamp(facilityAndTime: (Option[Long], Option[Seq[Facility]])): AsyncStream[TimeDecorator[Facility]] = {

   val facilityStream: Option[AsyncStream[TimeDecorator[Facility]]] = facilityAndTime._1.map { time =>  
        facilityAndTime._2.map { facilities => {
          AsyncStream.fromSeq(facilities).map { facility => new TimeDecorator(time, facility)}
        }}
    }.flatten 
    
    val ddd = facilityStream.getOrElse(AsyncStream.empty[AsyncStream[TimeDecorator[Facility]]]).asInstanceOf[AsyncStream[TimeDecorator[Facility]]]
    
    ddd
  }

}