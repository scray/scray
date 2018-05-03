package scray.core.service.util

import scray.core.service.spools.TimedSpoolRack
import org.mockito.Matchers._
import org.mockito.Mockito._
import scray.querying.Query
import com.twitter.concurrent.Spool
import scray.querying.description.Row
import scray.core.service.ScrayStatefulTServiceImpl
import scray.core.service.ScrayStatelessTServiceImpl
import scray.core.service.spools.memcached.MemcachedPageRack
import com.twitter.util.Await

trait MockedPlanner extends SpoolSamples {

  val mockplanner = mock(classOf[(Query) => Spool[Row]])
  object MockedSpoolRack extends TimedSpoolRack(planAndExecute = mockplanner)
  object MockedPageRack extends MemcachedPageRack(planAndExecute = mockplanner)

  when(mockplanner(anyObject())).thenReturn(Await.result(spoolOf8))

  object StatefulTestService extends ScrayStatefulTServiceImpl(MockedSpoolRack)
  object StatelessTestService extends ScrayStatelessTServiceImpl(MockedPageRack)

  def createMockedPlanner(rows : Int, maxCols : Int, vals : Array[_]) : (Query) => Spool[Row] = {
    val genMockPlanner = mock(classOf[(Query) => Spool[Row]])
    when(genMockPlanner(anyObject())).thenReturn(Await.result(spoolGen(rows, maxCols, vals)))
    genMockPlanner
  }

  object GenMockedSpoolRack {
    def apply(rows : Int, maxCols : Int, vals : Array[_]) =
      new TimedSpoolRack(planAndExecute = createMockedPlanner(rows, maxCols, vals))
  }

  object GenMockedPageRack {
    def apply(rows : Int, maxCols : Int, vals : Array[_]) =
      new MemcachedPageRack(planAndExecute = createMockedPlanner(rows, maxCols, vals))
  }

  object GenStatefulTestService {
    def apply(rows : Int, maxCols : Int, vals : Array[_]) =
      new ScrayStatefulTServiceImpl(GenMockedSpoolRack(rows, maxCols, vals))
  }

  object GenStatelessTestService {
    def apply(rows : Int, maxCols : Int, vals : Array[_]) =
      new ScrayStatelessTServiceImpl(GenMockedPageRack(rows, maxCols, vals))
  }

}