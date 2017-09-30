package topcustomers

import org.apache.spark.rdd.RDD
import common.Constants

/**
 * @author bli
 * @date Aug 13, 2017
 */
 
object Math {
  
  def computeTotal(transactions: RDD[String]): RDD[(String, Customer)] = {
    val total = transactions.map(i => {
      val sp = i.split(Constants.semicolon)
      (sp(0), sp(1).replaceAll("[^0-9.]+", "").toDouble)
    }).reduceByKey((a, b) => a + b).map(i => (i._1, Customer(i._1, null, null, Some(i._2))))
    total
  }
  
  def getTopCustomer(k:Int, customer: RDD[Customer]): Array[Customer] = {
    val sortCustomer = new Ordering[Customer] {
      override def compare(a: Customer, b: Customer) = {
        (a.total, b.total) match {
          case (Some(x), None) => +1
          case (None, Some(y)) => -1
          case (None, None)    => if (a.id > b.id) -1 else +1
          case (Some(x), Some(y)) =>
            if (x > y) {
              +1
            } else {
              -1
            }
        }
      }
    }
    
    customer.top(k)(sortCustomer)
  }
}