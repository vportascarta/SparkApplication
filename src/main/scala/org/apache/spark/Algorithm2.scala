package org.apache.spark

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.Models.Hyperedge
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object Algorithm2 {
  //Returns an array that contains the chosen sets
  /*
  Some considerations :
  https://stackoverflow.com/questions/1463284/hashset-vs-treeset
  https://github.com/JerryLead/SparkInternals/blob/master/markdown/english/6-CacheAndCheckpoint.md
  Repartition problems
  https://stackoverflow.com/questions/36414123/does-a-flatmap-in-spark-cause-a-shuffle
   */

  def greedy_algorithm(sc: SparkContext, rdd: RDD[Hyperedge]): ArrayBuffer[Long] = {
    //sc.setCheckpointDir(".") //not used when using local checkpoint
    val randomGen = scala.util.Random
    var currentRDD = rdd
    //.cache()
    val logEdgesChosen = ArrayBuffer[Long]()
    var counter = 1

    currentRDD.localCheckpoint()

    def loop(): Unit = {
      while (true) {
        currentRDD.localCheckpoint()

        CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
        counter += 1

        //Condition de fin, le RDD est vide
        if (currentRDD.isEmpty()) return

        //Trouver le sommet S qui est présent dans le plus de tTests (Transformation)
        val rdd_sommetsCount = currentRDD.mapPartitions(it => {
          it.flatMap(u => {
            var ret = new ArrayBuffer[(Long, Int)]()
            for (i <- u.listVertices) {
              ret += Tuple2(i, 1)
            }
            ret

          })
        })

        //Calculate the counts for each vertex (Transformation)
        val counts = rdd_sommetsCount.reduceByKey((a, b) => a + b)

        //Send random tiebreaker. Closest Long gets chosen in case of a tie.
        val tiebreaker = randomGen.nextLong() % 10

        //Find best vertex to cover the set (Action)
        val best = counts.reduce((a, b) => {
          var retValue = a
          //If same strength, we have to compare to the tiebreaker.
          if (a._2 == b._2) {
            val distanceA = Math.abs(tiebreaker - (a._1 % 10))
            val distanceB = Math.abs(tiebreaker - (b._1 % 10))
            if (distanceA > distanceB)
              retValue = b
            else retValue = a
          }
          else if (a._2 > b._2) retValue = a else retValue = b
          retValue
        })

        //Keep our chosen Set in the log
        logEdgesChosen.append(best._1)

        //Remove dead T-tests
        //best_1 will be shipped to each task but it is rather small.
        //No need to use a BC variable here
        currentRDD = currentRDD.flatMap(edge => {
          if (edge.listVertices.contains(best._1))
            None
          else Some(edge)
        })

        //Manually unpersist unused RDDs
        rdd_sommetsCount.unpersist(false)
        counts.unpersist(false)
      }
    }

    loop()
    logEdgesChosen
  }
}

