package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Models.node

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

abstract class Algorithm extends Serializable {
  //Returns the biggest color
  def getBiggestColor(g: Graph[node, String]): Int = {
    val aa = g.vertices.collect()
    var maxColor = 0
    for (i <- aa) {
      if (i._2.color > maxColor) maxColor = i._2.color
    }
    maxColor
  }

  def printGraphProper(g: Graph[node, String]) {
    g.vertices.collect().sortBy(_._1).foreach(vv => {
      val nts = vv._2.toString
      println("VertexId:" + vv._1 + s" $nts")
    })

    println("Printing edges as well")
    g.edges.collect().foreach(println)

  }

  def randomize_ids(g: Graph[node, String], sc: SparkContext): Graph[node, String] = {
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
      val n = node(cc, tiebreakvalue = ids_random(cc))
      cc += 1
      (v._1, n)
    })

    val vv = sc.makeRDD(vertices).localCheckpoint()
    val gg = Graph(vv, g.edges)
    gg
  }

  def execute(g: Graph[node, String], maxIterations: Int, sc: SparkContext): Graph[node, String]
}
