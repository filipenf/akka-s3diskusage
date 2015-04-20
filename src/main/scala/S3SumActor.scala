import akka.actor._
import akka.dispatch.PriorityGenerator
import akka.dispatch.UnboundedPriorityMailbox
import akka.routing.{RoundRobinRoutingLogic, Router, ActorRefRoutee}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectListing, ListObjectsRequest, S3ObjectSummary}
import com.typesafe.config.{ConfigFactory, Config}
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.duration._

case class ListingRequest(client: AmazonS3Client, bucketName : String, prefix : String)

case class ContinueListingRequest(originalRequest : ListingRequest, previousListing : ObjectListing)

case class KeyInfo(storageClass : String, keyName : String, size : Long)


class PriorityMailbox(settings: ActorSystem.Settings, config: Config) extends UnboundedPriorityMailbox (
    // Create a new PriorityGenerator, lower prio means more important
    PriorityGenerator {
      // 'highpriority messages should be treated first if possible
      case c: ContinueListingRequest => 0
      case _ => 1
    })

object Defaults {
  val PATH_SEPARATOR = "/"
  case object Shutdown
}

class Breakout {
  private lazy val breakouts = new mutable.HashMap[String, Breakout]() with scala.collection.mutable.SynchronizedMap[String, Breakout]
  private var double: Double = 0
  private var isComposite: Option[Boolean] = None

  def apply(key: String) = {
    require(isComposite.getOrElse(true), "This breakout was previously assigned a double value.")
    isComposite = Option(true)
    breakouts.getOrElseUpdate(key, new Breakout())
  }

  def +=(value: Double) = {
    require(!isComposite.getOrElse(false), "This breakout was previously assigned a composite value.")
    isComposite = Option(false)
    double += value
  }

  def total: Double = if (isComposite.getOrElse(false))
    breakouts.values.foldLeft(0.toDouble)(_ + _.total)
  else double

  override def toString = {
    val builder = new mutable.StringBuilder()
    appendToString(builder, 0)
    builder.toString()
  }

  private def appendToString(output: mutable.StringBuilder, level: Int): Unit = {
    def indent() = output ++= "  " * level
    def newline() = output ++= "\n"
    breakouts.toSeq.sortBy(-_._2.total) foreach {
      case (key, b) if b.isComposite.get =>
        indent()
        output.append(f"- $key (${b.total}%f):")
        newline()
        b.appendToString(output, level + 1)
      case (key, b) =>
        indent()
        output.append(f"- $key: ${b.double}%f")
        newline()
    }
  }
}

case class DirectoriesFound(count : Int)
case class DirectoryCompleted(path : String)

class Manager extends Actor with ActorLogging {

  var found : Int = 1
  var completed : Int = 0

  def receive = {
    case DirectoriesFound(subDirs)=> {
      found += subDirs
      log.info(s"+ $subDirs found ( total = $found / completed = $completed)")
    }
    case DirectoryCompleted(path) => {
      completed += 1
      log.info(s"Completed dir $path. ${found - completed} remaining")
      if (found - completed == 0) {
        context.actorSelection("/user/manager") ! Defaults.Shutdown
        context.actorSelection("/user/sumActor") ! Defaults.Shutdown
        context stop self
      }
    }
  }
}

class RoutingActor extends Actor with ActorLogging {

  var router = {
    val routees = Vector.fill(16) {
      val r = context.actorOf(Props[S3ListingActor].withMailbox("prio-mailbox"))
      context watch r
      ActorRefRoutee(r)
    }
    Router(RoundRobinRoutingLogic(), routees)
  }

  def receive = {
    case req : ContinueListingRequest => router.route(req, sender())
    case req : ListingRequest => router.route(req, sender())
    case Defaults.Shutdown => {
      router.routees.foreach(r => router.removeRoutee(r))
      context stop self
    }
  }

}

class S3ListingActor extends Actor with ActorLogging {

  val sumActor = context.actorSelection("/user/sumActor")
  val manager = context.actorSelection("/user/manager")
  val router = context.actorSelection("/user/router")

  def continueListing(clr : ContinueListingRequest): Unit = {
    val newListing = clr.originalRequest.client.listNextBatchOfObjects(clr.previousListing)
    processListing(clr.originalRequest, newListing)
  }

  def processListing(request : ListingRequest, listing : ObjectListing) = {
    log.debug(s"Prefix: ${listing.getPrefix} Common prefixes: ${listing.getCommonPrefixes} is_truncated: ${listing.isTruncated}")
    manager ! DirectoriesFound(listing.getCommonPrefixes.length)
    listing.getCommonPrefixes.foreach(item => {
      log.info(s"New request for prefix: $item")
      val name = item.split("/").lastOption.getOrElse("_").replace("/","_")
      if (name != "_") {
        router ! request.copy(prefix = item)
      }
    })
    listing.getObjectSummaries.iterator().foreach( item => {
      sumActor ! KeyInfo(item.getStorageClass, item.getKey, item.getSize)
    })
    if (listing.isTruncated) {
      log.info(s"${request.prefix} listing is truncated. Requesting more data...")
      listing.getObjectSummaries.clear()
      listing.setCommonPrefixes(null)
      router ! ContinueListingRequest(request, listing)
    } else {
      manager ! DirectoryCompleted(request.prefix)
    }
  }

  def receive = {
    case r: ContinueListingRequest => continueListing(r)
    case r: ListingRequest => {
      val listing = r.client.listObjects(new ListObjectsRequest().withBucketName(r.bucketName).
        withPrefix(r.prefix).withDelimiter(Defaults.PATH_SEPARATOR))
      processListing(r, listing)
    }
  }
}

class S3SumActor extends Actor with ActorLogging {

  import context._

  var breakout = new Breakout()

  override def preStart() = system.scheduler.schedule(30000 millis, 30000 millis, self, "report")

  def receive = {
   case KeyInfo (storageClass, keyName, keySize) => {
     val splitKey = keyName.split(Defaults.PATH_SEPARATOR)
     if (splitKey.length >= 2) {
       breakout(storageClass)(splitKey(0))(splitKey(1)) += keySize
     }
   }
   case "report" => log.info(breakout.toString)
   case Defaults.Shutdown => {
     println("Results:")
     println(breakout)
     context stop self
     system.shutdown()
   }
  }

}


object S3Usage extends App {
  private val credentials = new BasicAWSCredentials(args(0), args(1))

  private val clientConfig = new ClientConfiguration().withMaxConnections(10).withConnectionTimeout(120 * 1000).withMaxErrorRetry(20)

  private val amazonS3Client = new AmazonS3Client(credentials, clientConfig)

  val conf = ConfigFactory.parseString("""
      prio-mailbox {
        mailbox-type = "PriorityMailbox"
        //Other mailbox configuration goes here
      }
    """.stripMargin)

  val system = ActorSystem("S3DU", conf)
//  val listingActor = system.actorOf(Props[S3ListingActor], name = "listingActor")
  val sumActor = system.actorOf(Props[S3SumActor], name="sumActor")
  val manager = system.actorOf(Props[Manager], name="manager")
  val router = system.actorOf(Props[RoutingActor], name="router")

  router ! ListingRequest(amazonS3Client, args(2), "")


}