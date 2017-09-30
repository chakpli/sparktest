package topcustomers

import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._
import common.Constants

/**
 * @author bli
 * @date Aug 13, 2017
 */
 
object Filters {
  


  def filterHeader(rdd: RDD[String]): RDD[String] = {
    rdd.filter(s => !s.contains(Constants.customerId))
  }
  
  def filterBlackListEmail(customers: RDD[String], blacklist: RDD[String]): RDD[(String,Customer)] = {
    val _customers =  customers.flatMap(i => {
      val ab = new ArrayBuffer[(String, Option[Customer])]()
      
      val sp = i.split(Constants.semicolon) 
      if (sp.length == 3){
        for(email <- sp(2).split(Constants.comma)){
          var opt: Option[Customer] = Some(Customer(sp(0), sp(1), null, None))
          ab.append((email, opt))
        }
      }
      ab
    })
    
    val _blacklist = blacklist.map(i => {val opt:Option[Customer] = None; (i, opt)})

    val customersFilteredBlacklisted = _customers.union(_blacklist).groupByKey().flatMap(i => {
      val ab = new ArrayBuffer[(String, Customer)]()
      breakable {
        for (v <- i._2) {
          if (v.isEmpty) { println("found blacklisted email: " + i._1); ab.clear(); break }
          else {
            ab.append((v.get.id, Customer(null, v.get.name, i._1, None))) //customer id, name, email
          }
        }
      }
      ab
    }).groupByKey().map(i => {
      val ab = new ArrayBuffer[String]()
      val customers = i._2.toArray
      val name = customers(0).name
      for (customer <- customers) {
        ab.append(customer.emails)  
      }
      (i._1, Customer(i._1, name, ab.toArray.mkString(Constants.comma), None))
    })
    
    customersFilteredBlacklisted
  }
}