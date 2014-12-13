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
 * Finds anomalies in the timestamp of page views
 */
object Driver {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Find data anomalies")
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
        (split(0), (parser.parse(split(1)), split(2).toInt))
      })
      .groupByKey()
      .map({ case(title, data) =>
        val buf = ListBuffer.empty[(String, String)]
        val sorted = data.toArray.sortWith((elem1, elem2) => elem1._1.compareTo(elem2._1) > 1)

        if (sorted.length > elemCount) {
          var currentMean = sorted.take(elemCount).map(elem => elem._2).sum
          for (i <- elemCount to sorted.length - 1) {
            if (sorted(i)._2 > 150 && (sorted(i)._2 > 1.5 * (currentMean / elemCount) || sorted(i)._2 <  0.5 * (currentMean / elemCount))) {
              buf += ((title, sorted(i)._1.toString))
            }
            currentMean -= sorted(i - elemCount)._2
            currentMean += sorted(i)._2
          }
        }
        buf.iterator
      })
      .flatMap(a => a)
      .saveAsTextFile(outputDir)
  }
}
