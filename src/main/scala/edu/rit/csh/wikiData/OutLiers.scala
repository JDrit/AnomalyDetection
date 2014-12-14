package edu.rit.csh.wikiData

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._
import java.util.Date
import java.text.SimpleDateFormat
import scala.collection.mutable.ListBuffer

/**
 * Finds anomalies in the timestamp of page views. Groups the 
 * anomalies together in short ranges
 */
object OutLiers {
  
 
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Find data anomalies")
    val sc = new SparkContext(conf)
   
    if (args.length < 2) {
      println("Specify input and output directory")
      System.exit(1)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val elemCount = 5 //24 * 14   // compare each point to the last 2 weeks
    val parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val DAY_IN_MILLI: Long = 86400000L

    // input format: title , timestamp , views
    sc.textFile(inputDir)
      .map({line =>
        val split = line.split("\t")
        (split(0), (parser.parse(split(1)), split(2).toInt))
      })
      .groupByKey()
      .map({case(title, data) =>
        // the data points sorted by timestamp
        val sorted = data.toArray.sortWith((elem1, elem2) => elem1._1.before(elem2._1))
        
        // generate the local outlier factor for each data point
        val pts = (elemCount until sorted.length).map({ (i: Int) => 
            (title, (Detection.kDistance(i, elemCount, sorted), sorted(i)._1))
          })
          .filter({ case(_, (distance, _)) => distance > 2000 })
        // string representations of the group of anomalies
        val results = new ListBuffer[(String, (Int, Date, Date))]()

        if (!pts.isEmpty) {
          var curTitle = pts.head._1
          var curDistance = pts.head._2._1
          var beginDate = pts.head._2._2
          var endDate = pts.head._2._2

          pts.tail foreach { case(title, (distance, date)) =>
            if (date.getTime() - endDate.getTime() > DAY_IN_MILLI / 4) {
              results += ((title, (curDistance, beginDate, endDate)))
              curTitle = title
              curDistance = distance
              beginDate = date
              endDate = date
            } else {
              curDistance = if (curDistance < distance) distance else curDistance
              endDate = date
            }
          }
        }
        results.iterator
      })
      .flatMap(a => a)
      .map({case(title, (distance, start, end)) => title + "," + distance + "," + parser.format(start) + "," + parser.format(end) })
      .saveAsTextFile(outputDir)
  }
}
