package org.apache.spark
import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.Models.Hyperedge
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/*This is the Edge Cover hypergraph solver

We represent a T-way testing problem with the Edge Cover

 * Input is a file of n lines. A line is a hyperedge
  * A hyperedge is a test that covers a number (m) of values
  * We choose a hyperedge that tests the most values. We then remove these tested values from other hyperedges
  * We end the algorithm when everything has been tested.
  *
  * This algorithm is greedy, but there is some randomness.
  * By picking a random edge when others are equal, we get different results.
  * Spark's random shuffle already gives us a built-in random. By running the algorithm multiple times
  * on the same data, the hyperedges get shuffled in other places. Which means that when doing the a < b comparison,
  * we don't always have the same hyperedge in a and b.
  * */

object Algorithm {
  def greedy_algorithm(sc: SparkContext, rdd: RDD[Hyperedge]): ArrayBuffer[Long] = {

    var currentRDD = rdd
    val logEdgesChosen = ArrayBuffer[Long]()
    var counter = 1

    def loop(): Unit = {
      while (true) {

        //We destroy the RDD lineage at every iteration.
        currentRDD.localCheckpoint()

        CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
        counter += 1

        //Exit condition, the RDD is empty
        if (currentRDD.isEmpty()) return

        //LoggerForTest.logger.fine("TEST EMPTY FINISHED")

        //Find the hyperedge with the best count
        //Here, random shuffling gives us built-in randomness. But we could do more (ACTION)
        val smallEdge = currentRDD.reduce((a, b) => {
          if (a.listVertices.size > b.listVertices.size) a else b
        })

        //Mettre notre hyperedge choisie en broadcast variable (optimisation réseau)
        //Put the hyperedge in a broadcast variable
        val smallEdgeBC = sc.broadcast(smallEdge)

        //Keep it in the log
        logEdgesChosen.append(smallEdge.edgeID)

        //Here we apply a flatmap to remove elements from all sets using this smalledge (Transformation)
        currentRDD = currentRDD.flatMap(he => {
          var result = Seq[Hyperedge]()
          val updatedListVertices = he.listVertices -- smallEdgeBC.value.listVertices
          val id = he.edgeID
          if (updatedListVertices.nonEmpty) {
            result = result :+ Hyperedge(updatedListVertices, id)
          }
          result
        })

      }
    }

    loop()

    logEdgesChosen

  }
}