package common

import org.apache.spark.SparkConf

/**
 * @author bli
 * @date Aug 13, 2017
 */
 
object Constants {
  val rootDir = "./"
  val semicolon = ";"
  val comma = ","
  
  val customerId = "customerId"
  val appName = "exam"
  val sparkMaster = try{new SparkConf().get("spark.master")}catch{case _:Throwable => "local[4]"}
}