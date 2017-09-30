package topcustomers

import org.apache.spark.rdd.RDD

/**
 * @author bli
 * @date Aug 13, 2017
 */
 
object Joiner {
  def joinCustomerInfo(totals: RDD[(String, Customer)], customersFilteredBlacklisted:RDD[(String, Customer)]): RDD[Customer] = {
    totals.union(customersFilteredBlacklisted).groupByKey().flatMap(i => {
      val customer = i._2.toArray
      if (customer.length == 1) None
      else {
        val customer0 = customer(0)
         val customer1 = customer(1)
         if (customer0.total.isEmpty) Some(Customer(i._1, customer0.name, customer0.emails, customer1.total))
         else Some(Customer(i._1, customer1.name, customer1.emails, customer0.total))
      }
    })
  }
}