package org.sunbird.audit.func

import java.util
import java.util.UUID
import java.util.stream.Collectors

import org.apache.commons.lang3.StringUtils
import org.sunbird.audit.util.{Neo4JUtil, RedisUtil}
import redis.clients.jedis.Jedis

import collection.JavaConverters._

object LeafnodesCacheValidator {

  case class LeafNodesCache(identifier: String, exists: Boolean)

  val routesPath = "bolt://11.4.2.5:7687"
  val batchSize = 100

  def main(args: Array[String]): Unit = {
    implicit val neo4JUtil = new Neo4JUtil(routesPath, "domain")
    val publishedCollectionCount = getCount()
    println("Total published collections: " + publishedCollectionCount)
    var withoutCacheIds = new util.ArrayList[String]()
    implicit val redisUtil = new RedisUtil("11.4.2.31").getConnection(10)

    var offset = 0
    while (offset < publishedCollectionCount) {
      val toVal = offset + batchSize
      println(s"Processing from $offset to $toVal")
      val ids = getIds(offset, batchSize)
      offset += batchSize
      val noCacheList = ids.map(id => LeafNodesCache(id, hasLeafNodesKey(id))).filter(a => !a.exists)
      if (noCacheList.size > 0) {
        withoutCacheIds.addAll(noCacheList.map(a => a.identifier).asJava)
      }
    }
    println("Completed processing 'Leaf nodes cache validation'.")
    if (withoutCacheIds.size() > 0) {
      println("ALERT!... There are collections without cache. Count: "+ withoutCacheIds.size())
      println("Collections has no cache: " + withoutCacheIds)
      withoutCacheIds.asScala.map(id => {
        generateEvent(id)
      })
    } else {
      println("All the collections cache exists!...")
    }

    neo4JUtil.close()
    redisUtil.close()
  }

  def getCount()(implicit neo4JUtil: Neo4JUtil) = {
    val result = neo4JUtil.executeQuery("""MATCH (n:domain{IL_FUNC_OBJECT_TYPE: "Collection", mimeType: "application/vnd.ekstep.content-collection", visibility: "Default", contentType: "Course"}) WHERE n.status IN ["Live", "Unlisted"] RETURN COUNT(n.IL_UNIQUE_ID) as CollectionCount;""")
    result.single().get("CollectionCount", 0)
  }

  def getIds(offset: Int, limit: Int)(implicit neo4JUtil: Neo4JUtil) = {
    val query = s"""MATCH (n:domain{IL_FUNC_OBJECT_TYPE:"Collection", mimeType: "application/vnd.ekstep.content-collection", visibility: "Default", contentType: "Course"}) WHERE n.status in ["Live", "Unlisted"] RETURN n.IL_UNIQUE_ID AS identifier SKIP $offset LIMIT $limit;"""
    val result = neo4JUtil.executeQuery(query)
    if (null != result && result.hasNext) {
      result.list().asScala.map(record => record.get("identifier", ""))
    } else List[String]()
  }

  def hasLeafNodesKey(identifier: String)(implicit redisUtil: Jedis): Boolean = {
    val key = s"$identifier:$identifier:leafnodes"
    redisUtil.exists(key)
  }

  def getMetadata(identifier: String)(implicit neo4JUtil: Neo4JUtil) = {
    val query = s"""MATCH (n:domain{IL_FUNC_OBJECT_TYPE: "Collection"}) WHERE n.IL_UNIQUE_ID="$identifier" RETURN n.trackable as trackable, n.createdFor as createdFor, n.createdBy as createdBy, n.name as name, n.pkgVersion as pkgVersion, n.status as status, n.channel as channel;"""
    val result = neo4JUtil.executeQuery(query)
    result.single().asMap()
  }

  def generateEvent(identifier: String)(implicit neo4JUtil: Neo4JUtil) = {
    val metadata = getMetadata(identifier)
    val uuid = UUID.randomUUID()
    val timestamp = System.currentTimeMillis()
    val trackable = metadata.get("trackable")
    println("Trackable:" + trackable)
  }



}
