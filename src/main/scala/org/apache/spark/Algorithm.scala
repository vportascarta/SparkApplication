package org.apache.spark

import ca.lif.sparklauncher.main.CustomLogger
import org.apache.spark.Models.Hyperedge
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object Algorithm {
  //Greedy algorithm here
  def greedy_algorithm(sc: SparkContext, rdd: RDD[Hyperedge]): ArrayBuffer[Long] = {

    var currentRDD = rdd
    val logEdgesChosen = ArrayBuffer[Long]()
    var counter = 1

    def loop(): Unit = {
      while (true) {

        currentRDD.localCheckpoint()

        CustomLogger.logger.info(s"ITERATION NUMBER : $counter")
        counter += 1

        //Condition de fin, le RDD est vide
        if (currentRDD.isEmpty()) return

        //LoggerForTest.logger.fine("TEST EMPTY FINISHED")

        //Trouver l'edge avec le meilleur count
        val smallEdge = currentRDD.reduce((a, b) => {
          if (a.listVertices.size > b.listVertices.size) a else b
        })

        //LoggerForTest.logger.fine("SMALLER EDGE FOUND")

        //Mettre notre hyperedge choisie en broadcast variable (optimisation réseau)
        val smallEdgeBC = sc.broadcast(smallEdge)

        //Garder l'edge dans le log
        logEdgesChosen.append(smallEdge.edgeID)

        //LoggerForTest.logger.fine("SMALLER EDGE BROADCAST")

        currentRDD = currentRDD.flatMap(he => {
          var result = Seq[Hyperedge]()
          val updatedListVertices = he.listVertices -- smallEdgeBC.value.listVertices
          val id = he.edgeID
          if (updatedListVertices.nonEmpty) {
            result = result :+ Hyperedge(updatedListVertices, id)
          }
          result
        })

        // currentRDD.take(1)

        //LoggerForTest.logger.info("END OF ITERATION")
      }
    }

    loop()

    logEdgesChosen

  }
}