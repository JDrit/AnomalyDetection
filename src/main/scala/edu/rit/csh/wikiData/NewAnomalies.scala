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
 * Find the highest anomalies for each title for the input.
 * This will run against the newest data to determine what articles have
 * the highest anomalies in the last n days.
 */
object NewAnomalies {
  
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Find new data anomalies")
    val sc = new SparkContext(conf)
   
    if (args.length < 2) {
      println("Specify input and output directory")
      System.exit(1)
    }

    val inputDir = args(0)
    val outputDir = args(1)
    val elemCount = 24
    val parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val DAY_IN_MILLI: Long = 86400000L

    // input format: title , timestamp , views
    sc.textFile(inputDir)
      .map({ line =>
        val split = line.split("\t")
        if (split.length == 3 && 
          parser.parse(split(1)).after(new Date(new Date().getTime() - 3 * DAY_IN_MILLI)))
          (split(0), (parser.parse(split(1)), split(2).toInt))
        else
          null
      })
      .filter(a => a != null)
      .groupByKey()
      .map({ case(title, data) =>
        if (data.minBy(_._2)._2 < 50) {
          (0, title)
        } else {
          // the data points sorted by timestamp
          val sorted = data.toArray.sortWith((elem1, elem2) => elem1._1.after(elem2._1))
          val max = (elemCount until sorted.length).map({ (i: Int) => 
            Detection.kDistance(i, elemCount, sorted) 
          })
          if (max.isEmpty) (0, title)
          else (max.sum, title)
        }
      })
      .filter({case(max, title) => max != 0})
      .sortByKey(false, 1)
      .saveAsTextFile(outputDir)
  }
}
