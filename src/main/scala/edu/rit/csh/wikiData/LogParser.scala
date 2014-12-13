package edu.rit.csh.wikiData

import akka.actor.{Actor, ActorRef, ActorSystem, ActorLogging, Props}
import akka.routing.RoundRobinPool

import java.io.{File, PrintWriter}
import org.apache.commons.io.{FileUtils,LineIterator}
import scala.sys.process._


/**
 * Runs throw the logs and creates a copy of only the english logs
 */
object LogParser {
  
  class ParseActor(outputDir: String, logger: ActorRef) extends Actor {

    def receive: Receive = {
      case file: File => {
        val pw = new PrintWriter(outputDir + file.getName())
        val itr = FileUtils.lineIterator(file, "UTF-8") 
        while (itr.hasNext) {
          val line = itr.next
          if (line.startsWith("en ")) pw.write(line + "\n")
        }
        itr.close()
        pw.close()
        logger ! file.getName()
      }
    }
  }

  /**
   * Logs how many files have been completed
   */
  class LogActor() extends Actor {
    var count = 0
    def receive: Receive = {
      case fileName: String => {
        count += 1
        println(count + ":\t" + fileName)
      }
    }
  }

  def main(args: Array[String]) {
    val inputDir = "/storage/wikidata/raw_data/"
    val outputDir = "/storage/wikidata/english_data/"
    
    val system = ActorSystem()
    val logger = system.actorOf(Props(new LogActor()))
    val actors = system.actorOf(Props(new ParseActor(outputDir, logger))
      .withRouter(RoundRobinPool(27)))
    new File(inputDir).listFiles map { file => actors ! file }
  }
}
