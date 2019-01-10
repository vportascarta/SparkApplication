package org.apache.spark.graphx

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Models.node

/* This version uses Subgraph at every iteration to remove knight nodes and knight edges*/

class AlgorithmFC2() extends Algorithm {
  def sendTieBreakValues(ctx: EdgeContext[node, String, Long]): Unit = {
    if (ctx.srcAttr.knighthood == false && ctx.dstAttr.knighthood == false)
      {
        ctx.sendToDst(ctx.srcAttr.tiebreakvalue)
        ctx.sendToSrc(ctx.dstAttr.tiebreakvalue)
      }
  }

  def selectBest(tbValue1: Long, tbValue2: Long): Long = {
    if (tbValue1 < tbValue2) tbValue1
    else tbValue2
  }

  def increaseColor(vid: VertexId, sommet: node, bestTieBreak: Long): node = {
    if (sommet.tiebreakvalue < bestTieBreak)
      node(sommet.id, sommet.color, knighthood = 1, sommet.tiebreakvalue)
    else {
      node(sommet.id, sommet.color + 1, knighthood = 0, sommet.tiebreakvalue)
    }
  }

  def execute(graph: Graph[Models.node, String], maxIterations: Int, sc: SparkContext): Graph[node, String] = {
    var myGraph = randomize_ids(graph, sc).cache()

    var counter = 0
    var checkpoint_counter = 0
    val fields = new TripletFields(true, true, false) //join strategy

    def loop1(): Unit = {
      while (true) {

        myGraph.checkpoint()

       // myGraph.vertices.localCheckpoint()
       // myGraph.edges.localCheckpoint()

        CustomLogger.logger.info("ITERATION NUMERO : " + (counter + 1))
       // CustomLogger.logger.info("Checkpoint baby")
        counter += 1
        //checkpoint_counter += 1

        if (counter == maxIterations) return

        val messages = myGraph.aggregateMessages[Long](
          sendTieBreakValues,
          selectBest,
          fields //use an optimized join strategy (we don't need the edge attribute)
        )

        //Action. Stop when there are no more messages
        if (messages.isEmpty()) return

        //Transformation
        myGraph = myGraph.joinVertices(messages)(
          (vid, sommet, bestId) => increaseColor(vid, sommet, bestId))

        //We make the graph smaller
//        myGraph = myGraph.subgraph(
//          et => {
//            if (et.srcAttr.knighthood || et.dstAttr.knighthood)
//              false
//            else true
//          }, vpred = (vid, node_data) => {
//            if (node_data.knighthood == true) false
//            else true
//          }
//        )

        //Print the size of the graph after our subgraph operation
        //CustomLogger.logger.info("NUMBER OF EDGES AFTER SUBGRAPH : " + (myGraph.edges.count() + 1))
        //CustomLogger.logger.info("NUMBER OF VERTICES AFTER SUBGRAPH : " + (myGraph.vertices.count() + 1))
      }
    }

    loop1() //execute loop
    myGraph //return the result graph
  }
}
