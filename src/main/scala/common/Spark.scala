package common

/**
 * @author bli
 * @date Aug 13, 2017
 */
 

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object Spark {

  private var sparkSession:SparkSession = null
  
  def getSparkSession(): SparkSession = synchronized {
    if (sparkSession == null){
      init(null)
      println("SparkSession init done")
    }
    sparkSession
  }
  
  def getSparkContext(): SparkContext = {
    getSparkSession().sparkContext
  }
  
  def getSqlContext(): SQLContext = {
    getSparkSession().sqlContext
  }

  private def init(ss: SparkSession): Unit = {
    if (sparkSession == null) {
      if (ss == null) {
        println("spark.master is set as:" + Constants.sparkMaster)
        val conf = new SparkConf()
          .setMaster(Constants.sparkMaster)
          .setAppName(Constants.appName)

        sparkSession = SparkSession.builder().config(conf).getOrCreate()

        } else {
        sparkSession = ss
      }
    }
  }

  def stop(): Unit = {
    if (sparkSession != null){
	    sparkSession.stop()
	    sparkSession = null
    }
	}
}