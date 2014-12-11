package scray.core.service.util

import scray.core.service.spools.TimedSpoolRack
import org.mockito.Matchers._
import org.mockito.Mockito._
import scray.querying.Query
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import scray.core.service.ScrayTServiceImpl

trait MockedPlanner extends SpoolSamples {

  val mockplanner = mock(classOf[(Query) => Spool[Row]])
  object MockedSpoolRack extends TimedSpoolRack(planAndExecute = mockplanner)

  when(mockplanner(anyObject())).thenReturn(spoolOf8.get)

  object TestService extends ScrayTServiceImpl(MockedSpoolRack)

}