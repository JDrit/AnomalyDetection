package edu.rit.csh.wikiData

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

/**
 * Finds anomalies in the timestamp of page views
 */
object KMeansFinder {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("MLLib k-means")
    val sc = new SparkContext(conf)
   
    if (args.length < 1) {
      println("Specify input directory")
      sys.exit(1)
    }

    val inputDir = args(0)
    val parser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
 
    val parsedData = sc.textFile(inputDir).map({ line =>
      val split = line.split("\t")
      (parser.parse(split(1)).getTime() / 1000, split(2).toLong)  
    })
    .groupByKey()
    .map({ case(timestamp, views) =>
      Vectors.dense(timestamp, views.sum)
    })
    .cache() 
       
    val clusters = KMeans.train(parsedData, 5, 20) 

    val WSSSE = clusters.computeCost(parsedData)

    clusters.clusterCenters.foreach(c => println("center ", c))

  }
}
