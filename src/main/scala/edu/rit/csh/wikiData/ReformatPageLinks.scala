package edu.rit.csh.wikiData

import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

/**
 * Reformats the page links format from [page] [list of links]
 * to a list of vertices and a list of edges. The list of vertices
 * are formated as (unique id, title) and the list of edges are
 * (ID 1, ID 2)
 */
object ReformatPageLinks {
  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("Find data anomalies")
    val sc = new SparkContext(conf)
    
    if (args.length < 2) {
      println("Specify input and output directories")
      System.exit(1)
    }

    val inputDir = args(0)
    val outputDir = args(1)

    val vertices =  sc.textFile(inputDir)
      .map(line => line.split("\t").head)
      .distinct()
      .zipWithUniqueId()
   
    val edges = sc.textFile(inputDir)
      .map({ line => 
        val split = line.split("\t")
        split.tail.map(elem => (split.head, elem))
      })
      .flatMap(a => a)
      .join(vertices)
      .map({  case(title, (otherTitle, id)) =>
        (otherTitle, id)
      })
      .join(vertices)
      .map({ case(otherTitle, (firstId, otherId)) =>
        firstId + "\t" + otherId 
      })
      .distinct()
      .saveAsTextFile(outputDir + "/edges")
    
    vertices.map({ case(title, id) => id + "\t" + title })
      .saveAsTextFile(outputDir + "/verticies")

  }
}
