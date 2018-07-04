package scray.hdfs.io.write

import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec

class WriteResultFuture(state: WriteState) extends Future[WriteResult] {

  var timeOutTime = 0L // Timestamp when get operation will fail

  def cancel(x$1: Boolean): Boolean = {
    false
  }

  def get(time: Long, unit: TimeUnit): scray.hdfs.io.write.WriteResult = {

    timeOutTime = (System.currentTimeMillis() + unit.toMillis(time))

    @tailrec
    def wait() {
      if (timeOutTime > System.currentTimeMillis || !state.isCompleted) {
        Thread.sleep(1000)
        wait
      }
    }

    wait
    state.completedState
  }

  def get(): scray.hdfs.io.write.WriteResult = {
    @tailrec
    def wait() {
      if (!state.isCompleted) {
        Thread.sleep(1000)
        wait
      }
    }

    wait
    state.completedState
  }

  def isCancelled(): Boolean = {
    false
  }

  def isDone(): Boolean = {
    state.isCompleted
  }
}