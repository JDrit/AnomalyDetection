package edu.rit.csh.wikiData

import java.util.Date

/**
 * All the anomaly detection methods
 */
object Detection {
  /**
   * Determine the average distance from the kth previous data points
   *
   * @param index the current index to compute the distance for
   * @param elemCount the number of points to compare against
   * @param data all the data points
   * @return the Kth Distance
   */
  def kDistance(index: Int, elemCount: Int, data: Array[(Date, Int)]): Int = {
    (index - elemCount until index).map({ i => 
      ((data(index)._2 - data(i)._2) / data(index)._2).abs
    }).sum / elemCount
  }
  
  /**
   * Does Detection based off of a weighted average for each data point
   * with newer data points getting more weight
   */
  def weightedValue(index: Int, elemCount: Int, data: Array[(Date, Int)]): Int = {
    (1 to elemCount).map(i => 
        (data(index - i)._2 * (elemCount - i))
    ).sum / (1 to elemCount).map(i => i).sum
  }

  /**
   * Does detection based off of a exponential weight with newer points
   * getting weighted hevier
   */
  def expValue(index: Int, elemCount: Int, data: Array[(Date, Int)]): Int = {
    val weightedMult = 2 / (elemCount + 1)
  
    def ema(index: Int, valsLeft: Int): Int = valsLeft match {
      case 1      => data(index)._2
      case n: Int => {
        val prev = ema(index - 1, valsLeft - 1) 
        prev + weightedMult * (data(index)._2 - prev)
      }
    }
    ema(index, elemCount)
  }

}
