package src.main.scala.scray.querying.sync.conf

import scala.beans.BeanProperty

class SyncConfiguration {
  
  @BeanProperty var useLightweightTransactions: Boolean = false
}