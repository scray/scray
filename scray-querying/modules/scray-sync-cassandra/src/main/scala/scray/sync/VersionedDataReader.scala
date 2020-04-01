package scray.sync

import scala.util.Try
import scray.sync.api.VersionedData
import scala.util.Failure
import scala.collection.mutable.LinkedList

class VersionedDataReader {
  def get(): Try[Iterator[VersionedData]] = {
    Try((new LinkedList[VersionedData]).iterator)
  }
}