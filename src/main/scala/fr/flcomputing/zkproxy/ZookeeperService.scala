package fr.flcomputing.zkproxy

import java.util.Collections

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.{Id, ACL, Stat}
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

/**
  * Date: 29/01/2016
  *
  * @author François LAROCHE
  */
class ZookeeperService {

  case class Node(path: String, metadata: NodeMetadata, value: String)
  case class NodeMetadata(
                           creationCommitId: Long,
                           modificationCommitId: Long,
                           creationTime: Long,
                           modificationTime: Long,
                           version: Int,
                           childrenVersion: Int,
                           aversion: Int,
                           ephemeralOwner: Long,
                           dataLength: Int,
                           childrenCount: Int,
                           lastChildModificationCommitId: Long
                         )

  private val zookeepers = System.getProperty("ZOOKEEPERS", "localhost:2181")

  private val client = new ZooKeeper(zookeepers, 2000, new Watcher {
    override def process(event: WatchedEvent) = {
      println(event)
    }
  })

  def list(directory: String): java.util.List[String] = {
    client.getChildren(directory, false)
  }

  def detail(node: String): Node = {
    val stat = new Stat()
    val data: Array[Byte] = client.getData(node, false, stat)
    Node(
      path = node,
      value = Option(data).map(d => new String(d, "UTF-8")).orNull,
      metadata = NodeMetadata(
        creationCommitId = stat.getCzxid,
        creationTime = stat.getCtime,
        modificationCommitId = stat.getMzxid,
        modificationTime = stat.getMtime,
        version = stat.getVersion,
        childrenVersion = stat.getCversion,
        aversion = stat.getAversion,
        ephemeralOwner = stat.getEphemeralOwner,
        dataLength = stat.getDataLength,
        childrenCount = stat.getNumChildren,
        lastChildModificationCommitId = stat.getPzxid
      )
    )
  }

  def createNode(path: String, value: Option[String]) = {
    import scala.collection.JavaConversions._
    if(client.exists(path, false) == null) {
      client.create(path, value.map(_.getBytes("UTF-8")).orNull, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
    }
  }

}

object test extends App {
  import scala.collection.JavaConversions._

  val service = new ZookeeperService

  service.createNode("/test", None)
  service.createNode("/test/toto", Some("Hastalavista, baby"))

  def recursivePrint(path: String): Unit = {
    val metadata = service.detail(path)
    println(s"Node is: $metadata")
    val children = service.list(path)
    println(s"$path has children: ${children.mkString(", ")}")
    children.foreach(c => recursivePrint(if(path endsWith "/") path + c else path + "/" + c))
  }
  recursivePrint("/")

}

