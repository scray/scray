package de.s_node

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import com.twitter.concurrent.AsyncStream
import com.twitter.concurrent.Spool
import com.twitter.util.Future
import com.twitter.io.Reader
import com.twitter.util.Await


@RunWith(classOf[JUnitRunner])
class AsyncStreamTests extends WordSpec {
  
  "AssyncStream " should {
    " create stream" in {
      
      // Simulate database select result
      val dataIter = (1 :: 2 :: 3 :: 4 :: 5 :: Nil).iterator      
      def getNext: Option[Future[Int]] = {
        if(dataIter.hasNext) {
          Some(Future(dataIter.next()))
        } else {
          None
        }
      }
      
      // Create stream from iterator data
      def mkStream(): AsyncStream[Int] = {
        getNext  match {
          case Some(facility) =>  AsyncStream.fromFuture(facility) ++ mkStream()
          case None => AsyncStream.empty
        }
      }
      val stream = AsyncStream.fromFuture(Future(dataIter.next())) ++ mkStream()
      
      // Generate a string representation
      assert(Await.result(stream.foldLeft("")((acc, item) => acc + item)) === "12345")      
    }
  }
  
}