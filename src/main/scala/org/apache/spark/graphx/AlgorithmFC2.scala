package org.apache.spark.graphx

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Models.Node


class AlgorithmFC2(checkpointInterval: Int) extends Algorithm {
  def sendTieBreakValues(ctx: EdgeContext[Node, String, Long]): Unit = {
    if (!ctx.srcAttr.knighthood && !ctx.dstAttr.knighthood) {
      ctx.sendToDst(ctx.srcAttr.tiebreakingValue)
      ctx.sendToSrc(ctx.dstAttr.tiebreakingValue)
    }
  }

  def selectBest(tbValue1: Long, tbValue2: Long): Long = {
    if (tbValue1 < tbValue2) tbValue1
    else tbValue2
  }

  def increaseColor(vid: VertexId, sommet: Node, bestTieBreak: Long): Node = {
    if (sommet.tiebreakingValue < bestTieBreak)
      Node(sommet.id, sommet.color, knighthood = true, sommet.tiebreakingValue)
    else {
      Node(sommet.id, sommet.color + 1, knighthood = false, sommet.tiebreakingValue)
    }
  }

  def execute(graph: Graph[Models.Node, String], maxIterations: Int, sc: SparkContext): Graph[Node, String] = {
    var myGraph = randomize_ids(graph, sc)
    var counter = 0
    var checkpoint_counter = 0
    val fields = new TripletFields(true, true, false) //join strategy


    def loop1(): Unit = {
      while (true) {

        //Checkpoint the graph at each iteration, because we don't need it
        if (checkpoint_counter == checkpointInterval) {
          myGraph.vertices.checkpoint()
          checkpoint_counter = 0
        }

        CustomLogger.logger.info("ITERATION NUMERO : " + (counter + 1))
        counter += 1
        checkpoint_counter += 1
        if (counter == maxIterations) return

        val messages = myGraph.aggregateMessages[Long](
          sendTieBreakValues,
          selectBest,
          fields //use an optimized join strategy (we don't need the edge attribute)
        ).cache() //call cache to keep messages in memory please

        if (messages.isEmpty()) return

        myGraph = myGraph.joinVertices(messages)(
          (vid, sommet, bestId) => increaseColor(vid, sommet, bestId))

        //Ignorez : Code de debug
        // var printedGraph = myGraph.vertices.collect()
        // printedGraph = printedGraph.sortBy(_._1)
        // printedGraph.foreach(
        //   elem => println(elem._2)
        //  )

      }
    }

    loop1() //execute loop
    myGraph //return the result graph
  }
}
