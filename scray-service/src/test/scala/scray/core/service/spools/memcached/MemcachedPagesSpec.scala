package scray.core.service.spools

import org.junit.runner.RunWith
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.when
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.mock.MockitoSugar
import com.twitter.concurrent.Spool
import scray.core.service.parser.TQueryParser
import scray.core.service.util.SpoolSamples
import scray.core.service.util.TQuerySamples
import scray.querying.Query
import scray.querying.description.Row
import org.scalatest.junit.JUnitRunner
import com.twitter.util.Duration
import com.twitter.util.JavaTimer
import scray.core.service.KryoPoolRegistration
import scray.common.serialization.KryoPoolSerialization
import scray.service.qmodel.thrifscala.ScrayTQuery
import scray.core.service._
import scray.service.qmodel.thrifscala.ScrayTRow

@RunWith(classOf[JUnitRunner])
class SerializationSpec
  extends FlatSpec
  with Matchers
  with SpoolSamples
  with KryoPoolRegistration {

  register
  
  "Spool Serialization" should "serialize pages" in {    
  }

  it should "serialize page keys" in {
  }

}
