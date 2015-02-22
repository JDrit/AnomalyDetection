package edu.rit.csh.wikiData

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import java.util.Date
import java.text.SimpleDateFormat


/**
 * Finds the total views of articles and sorts them
 */
object TotalViews {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Total view finder")
    val sc = new SparkContext(conf)
   
    if (args.length < 2) println("Specify input and output directory")

    val inputDir = args(0)
    val outputDir = args(1)
    val elemCount = 4
    val parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    // ex: title timestamp views
    sc.textFile(inputDir)
      .map({line =>
        val split = line.split("\t")
        (split(0), split(2).toInt)
      })
      .groupByKey()
      .map({ case(title, data) => (data.sum, title) })
      .sortByKey(true, 1)
      .saveAsTextFile(outputDir)
  }
}
