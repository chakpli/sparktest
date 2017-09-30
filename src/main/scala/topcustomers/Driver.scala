package topcustomers

import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import common.Constants
import common.Spark
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

/**
 * @author bli
 * @date Aug 13, 2017
 */

object Driver {

  def main(arg: Array[String]): Unit = {

    var bw: BufferedWriter = null

    try {
      //put your files into these dirs
      val transactions = Filters.filterHeader(Spark.getSparkContext().textFile(Constants.rootDir + "/transactions"))
      val customers = Filters.filterHeader(Spark.getSparkContext().textFile(Constants.rootDir + "/customers"))
      val blacklist = Filters.filterHeader(Spark.getSparkContext().textFile(Constants.rootDir + "/blacklist"))
      val customersFilteredBlacklisted = Filters.filterBlackListEmail(customers, blacklist)

      val totals = Math.computeTotal(transactions)
      val topCustomers = Math.getTopCustomer(10000, Joiner.joinCustomerInfo(totals, customersFilteredBlacklisted))

      val file = new File(Constants.rootDir + "/output.txt")
      file.delete()
      bw = new BufferedWriter(new FileWriter(file))
      topCustomers.map(i =>
        i.total match {
          case Some(t) =>
            bw.write(
              i.id + Constants.comma +
                i.name + Constants.comma +
                i.emails + Constants.comma +
                t + "\n")
          case None =>
        })
        
    } finally {
      if (bw != null)
        bw.close()
      Spark.stop()
    }
  }
}