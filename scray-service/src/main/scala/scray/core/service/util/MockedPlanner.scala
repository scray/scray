package scray.core.service.util

import scray.core.service.spools.TimedSpoolRack
import org.mockito.Matchers._
import org.mockito.Mockito._
import scray.querying.Query
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import scray.core.service.ScrayStatefulTServiceImpl

trait MockedPlanner extends SpoolSamples {

  val mockplanner = mock(classOf[(Query) => Spool[Row]])
  object MockedSpoolRack extends TimedSpoolRack(planAndExecute = mockplanner)

  when(mockplanner(anyObject())).thenReturn(spoolOf8.get)

  object TestService extends ScrayStatefulTServiceImpl(MockedSpoolRack)

  def createMockedPlanner(rows : Int, maxCols : Int, vals : Array[_]) : (Query) => Spool[Row] = {
    val genMockPlanner = mock(classOf[(Query) => Spool[Row]])
    when(genMockPlanner(anyObject())).thenReturn(spoolGen(rows, maxCols, vals).get)
    genMockPlanner
  }

  object GenMockedSpoolRack {
    def apply(rows : Int, maxCols : Int, vals : Array[_]) =
      new TimedSpoolRack(planAndExecute = createMockedPlanner(rows, maxCols, vals))
  }

  object GenTestService {
    def apply(rows : Int, maxCols : Int, vals : Array[_]) =
      new ScrayStatefulTServiceImpl(GenMockedSpoolRack(rows, maxCols, vals))
  }

}