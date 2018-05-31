///* Programmation de l'algorithme FC1 (sans GraphX)
//*/
//
//package org.apache.spark.graphx
//import org.apache.spark.SparkContext
//
//import scala.collection.mutable.ArrayBuffer
//
//case class node(size : Int)
//{
//  //We declare the adjlist
//  var adjlist = new Array[Int](size)
//  var knighthood = false
//  var color : Int = 1
//  var tiebreaker : Int = 0
//}
//
//class AlgorithmFC4 extends Algorithm
//{
//  override def execute(g: Graph[Models.Node, String], maxIterations: PartitionID, sc: SparkContext): Graph[Models.Node, String] =
//  {
//
//    val cc = g.vertices.count().toInt
//    //Transfer Graph object to our object
//    var myGraph: Graph[node, String] = g.mapVertices((vid, nodee) => {
//      var gg = node( cc)
//      gg.knighthood = false
//      gg.color = 1
//      gg.tiebreaker = 0
//      gg
//    })
//
//    var messages = myGraph.aggregateMessages[ArrayBuffer[Long]] ( edge => {
//      var msga = new ArrayBuffer[Long]()
//      var msgb = new ArrayBuffer[Long]()
//      msga += edge.srcId
//      msgb += edge.dstId
//      edge.sendToDst(msga)
//      edge.sendToSrc(msgb)
//    },
//      (a,b) =>  a ++ b
//    )
//
//    //Fill in the adjmatrix from our messages
//    myGraph.joinVertices(messages) (  (vid, node, msgs) => {
//      var newnode = node
//      //Fill in the adj matrix
//      for (iii <- msgs) {
//              newnode.adjlist(iii) = 1
//          }
//      newnode
//  })
//
//  //Now we can use this graph for graph coloring
//
//
//
//  }
//
//
//
//}
//
//
//
////    type tt = Tuple2[VertexId,VertexId]
////
////    //Fill the array of nodes with the data from the edges.
////    var msgs = myGraph.edges.flatMap( e => {
////      var results = new ArrayBuffer[tt]
////      results += new tt(e.dstId,e.srcId)
////      results += new tt(e.srcId,e.dstId)
////      results
////    })
//
//
//
////    myGraph.vertices.join(msgs).map(old =>  {
////          old.
////
////    }
