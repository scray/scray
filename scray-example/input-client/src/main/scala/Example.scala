import io.swagger.client.ApiInvoker
import io.swagger.client.api.DefaultApi
import java.util.concurrent.LinkedBlockingQueue
import io.swagger.client.model.Facility
import io.swagger.client.api.DbAdamClient


object Example {
  def main(args: Array[String]): Unit = {
    val queue = new LinkedBlockingQueue[Facility]
    val client = new DbAdamClient(queue, 5000).start()
    
    Thread.sleep(10000)
  }
}