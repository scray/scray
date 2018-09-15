package scray.hdfs.io.write

import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.SettableFuture

class ScrayListenableFuture extends ListenableFuture[WriteResult] {
 
  val future = SettableFuture.create[WriteResult]();
  
  def this(succesResult: WriteResult) {
    this
    this.future.set(succesResult)
  }
  
  def this(failureResult: Throwable) {
    this
    this.future.setException(failureResult)
  }
  
  def cancel(x: Boolean): Boolean = future.cancel(x)
  def addListener(x1: Runnable, x2: java.util.concurrent.Executor): Unit = future.addListener(x1, x2)
  def get(x1: Long,x2: java.util.concurrent.TimeUnit): scray.hdfs.io.write.WriteResult = future.get(x1, x2)
  def get(): scray.hdfs.io.write.WriteResult = future.get
  def isCancelled(): Boolean = future.isCancelled()
  def isDone(): Boolean = future.isDone()
}