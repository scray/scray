// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scray.hdfs.io.write

import com.google.common.util.concurrent.ListenableFuture
import com.google.common.util.concurrent.SettableFuture

class ScrayListenableFuture[T] extends ListenableFuture[T] {
 
  val future = SettableFuture.create[T]();
  
  def this(succesResult: T) {
    this
    this.future.set(succesResult)
  }
  
  def this(failureResult: Throwable) {
    this
    this.future.setException(failureResult)
  }
  
  def cancel(x: Boolean): Boolean = future.cancel(x)
  def addListener(x1: Runnable, x2: java.util.concurrent.Executor): Unit = future.addListener(x1, x2)
  def get(x1: Long,x2: java.util.concurrent.TimeUnit): T = future.get(x1, x2)
  def get(): T = future.get
  def isCancelled(): Boolean = future.isCancelled()
  def isDone(): Boolean = future.isDone()
}