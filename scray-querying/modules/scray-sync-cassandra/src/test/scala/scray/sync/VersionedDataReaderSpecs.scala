package scray.sync

import org.junit.runner.RunWith
import org.scalatest.WordSpec

class VersionedDataReaderSpecs extends WordSpec {
  "VersionedDataReader" should {
    "handle io errors " in {
      val reader = new VersionedDataReader

      val result = reader.get().map(fff => fff.foldLeft("")((acc, x )=> {
        ""
      })
      )

    }
  }
}