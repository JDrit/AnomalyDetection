package edu.rit.csh.wikiData

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.graphx._

object PageRank {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Find data anomalies")
    val sc = new SparkContext(conf)

    val inputDir = args(0)
    val outputDir = args(1)

    val graph = GraphLoader.edgeListFile(sc, inputDir + "/edges")

    val ranks = graph.pageRank(0.0001).vertices

    val pages = sc.textFile(inputDir + "/vertices").map { line => 
      val split = line.split("\t")
      (split(0).toLong, split(1))
    }

    val ranksByPage = pages.join(ranks).map {
      case(id, (title, rank)) => (title, rank)
    }

    ranksByPage.saveAsTextFile(outputDir + "/ranks") 

  }
}
