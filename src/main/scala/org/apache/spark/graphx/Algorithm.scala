package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Models.Node

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

abstract class Algorithm extends Serializable {
  //Returns the biggest color
  def getBiggestColor(g: Graph[Node, String]): Int = {
    val aa = g.vertices.collect()
    var maxColor = 0
    for (i <- aa) {
      if (i._2.color > maxColor) maxColor = i._2.color
    }
    maxColor
  }

  def printGraphProper(g: Graph[Node, String]) {
    g.vertices.collect().sortBy(_._1).foreach(vv => {
      val nts = vv._2.toString
      println("VertexId:" + vv._1 + s" $nts")
    })
  }

  def randomize_ids(g: Graph[Node, String], sc: SparkContext): Graph[Node, String] = {
    val count: Int = g.vertices.count().toInt
    //Preparer un graph random
    //Randomizer les ids
    var ids_random: ArrayBuffer[Int] = new ArrayBuffer()
    //val seed = Random.nextInt(100)
    // println("le seed est : "+seed)
    // Random.setSeed(seed)
    Random.shuffle(1 to count).copyToBuffer(ids_random)
    var cc = 0
    var vertices = g.vertices.collect()
    vertices = vertices.map(v => {
      val n = Node(cc, tiebreakingValue = ids_random(cc))
      cc += 1
      (v._1, n)
    })

    val gg = Graph(sc.makeRDD(vertices), g.edges)
    //  printGraphProper(gg)
    gg
  }

  def execute(g: Graph[Node, String], maxIterations: Int, sc: SparkContext): Graph[Node, String]
}
