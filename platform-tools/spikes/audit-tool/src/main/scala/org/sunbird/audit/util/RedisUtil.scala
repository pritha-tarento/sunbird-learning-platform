package org.sunbird.audit.util



import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

class RedisUtil(host: String = "localhost", port: Int = 6379) extends java.io.Serializable {

  private val serialVersionUID = -396824011996012513L

  val redisHost: String = host
  val redisPort: Int = port
  private val logger = LoggerFactory.getLogger(classOf[RedisUtil])

  private def getConnection(backoffTimeInMillis: Long): Jedis = {
    val defaultTimeOut = 30000
    if (backoffTimeInMillis > 0) try Thread.sleep(backoffTimeInMillis)
    catch {
      case e: InterruptedException =>
        e.printStackTrace()
    }
    logger.info("Obtaining new Redis connection...")
    new Jedis(redisHost, redisPort, defaultTimeOut)
  }


  def getConnection(db: Int, backoffTimeInMillis: Long): Jedis = {
    val jedis: Jedis = getConnection(backoffTimeInMillis)
    jedis.select(db)
    jedis
  }

  def getConnection(db: Int): Jedis = {
    val jedis = getConnection(db, backoffTimeInMillis = 0)
    jedis.select(db)
    jedis
  }

  def getConnection: Jedis = getConnection(db = 0)
}