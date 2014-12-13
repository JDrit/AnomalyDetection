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
 * Find the highest anomalies for each title for the input
 */
object NewAnomalies {
  
  /**
   * Determine the average distance from the kth previous data points
   *
   * @param index the current index to compute the distance for
   * @param elemCount the number of points to compare against
   * @param data all the data points
   * @return the Kth Distance
   */
  def kDistance(index: Int, elemCount: Int, data: Array[(Date, Int)]): Int = {
    (index - elemCount until index).map(i => (data(index)._2 - data(i)._2).abs).sum / elemCount
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Find data anomalies")
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
      .map({line =>
        val split = line.split("\t")
        if (split.length == 3)
          (split(0), (parser.parse(split(1)), split(2).toInt))
        else
          null
      })
      .filter(a => a != null)
      .groupByKey()
      .map({case(title, data) =>
        // the data points sorted by timestamp
        val sorted = data.toArray.sortWith((elem1, elem2) => elem1._1.before(elem2._1))
        val max = (elemCount until sorted.length).map({ (i: Int) => kDistance(i, elemCount, sorted) })
        if (max.isEmpty) (title, 0)
        else (title, max.max)
      })
      .filter({case(title, max) => max != 0})
      .map({case(title, distance) => title + "\t" + distance})
      .saveAsTextFile(outputDir)
  }
}