package org.apache.spark.graphx

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.graphx.Models.node
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object ColoringProgram {


  //Generate random tiebreakers for an array of vertices
  def random_tiebreakers(v : Array[(Long, node_data)] ): Array[(Long, node_data)] =
  {

    val count = v.size
    var vertices = v

    //Preparer un graph random
    //Randomizer les ids
    var ids_random: ArrayBuffer[Int] = new ArrayBuffer()
    //val seed = Random.nextInt(100)
    // println("le seed est : "+seed)
    // Random.setSeed(seed)
    Random.shuffle(1 to count).copyToBuffer(ids_random)

    var cc = 0
    vertices = vertices.map(v => {
      val n = node_data(tiebreakvalue = ids_random(cc))
      cc += 1
      (v._1, n)
    })

  //  println("Print le array avec random tiebreakers")
   // vertices.sortBy(_._1).foreach( println)


    vertices
  }

  //Returns the biggest color
  def getBiggestColor_2( v : Array[(Long, node_data)] ): Int = {

    var maxColor = 0
    for (i <- v) {
      if (i._2.color > maxColor) maxColor = i._2.color
    }
    maxColor
  }


  def exec_for_gc(coloring : Algorithm, graph: Graph[Models.node, String] , sc : SparkContext): Unit =
  {
    val res = coloring.execute(graph, 1000, sc)
    val result = s"L'algorithme greedy a choisi ${coloring.getBiggestColor(res)} couleurs."
    CustomLogger.logger.info(result)
  }


  def exec_for_gc2( vertices: Array[(Long, node_data)], edges: Vector[edge_data], sc : SparkContext): Unit =
  {
    val algo = new BCastColoring()
    //Generate tiebreakers
    var myVertices = random_tiebreakers(vertices)
    val res: (algo.node, algo.edge) = algo.execute( vertices = sc.makeRDD(myVertices), e = sc.makeRDD(edges), sc)
    val result = s"L'algorithme greedy a choisi ${getBiggestColor_2(res._1.collect())} couleurs."
    CustomLogger.logger.info(result)
  }


  def launch(filepath: String,
             is_graphviz: Boolean,
             t: Int,
             n: Int,
             v: Int,
             partitions: Int,
             algo_version: Int,
             checkpoint_interval: Int,
             loops: Int,
             max_iterations: Int): Unit = {

    // Logging
    CustomLogger.setLogger()
    CustomLogger.logger.info("Graph Coloring - Beginning")
    CustomLogger.logger.info(s"Parameters : partitions = $partitions / " +
      s"version = $algo_version / ck_interval = $checkpoint_interval / max_iterations = $max_iterations")

    // Spark config
    val conf = new SparkConf().setAppName("Graph Coloring greedy").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    sc.setCheckpointDir("./")
    sc.setLogLevel("ERROR")
//
//    val graph: Graph[Models.node, String] = {
//      if (filepath.isEmpty) {
//        CustomLogger.logger.info(s"Config : T = $t / N = $n / V = $v")
//        Generator.generateGraph(t, n, v, sc, partitions)
//      }
//      else {
//        CustomLogger.logger.info(s"File : $filepath")
//        // Parsing
//        if (is_graphviz)
//          Parser.readGraphVizFile(filepath, sc, partitions)
//        else
//          Parser.readGraphFile(filepath, sc, partitions)
//      }
//    }
//
//    val coloring: Algorithm = {
//      if (algo_version == 1)
//        new AlgoColoring()
//      else if (algo_version == 2)
//        new AlgorithmFC2()
//      else if (algo_version == 3)
//        new AlgoColoring()
//      else
//        throw new RuntimeException("Wrong version")
    //


    //Knights and Peasants GraphX,  algo FC2 normal.
    if (algo_version == 1 || algo_version ==2)
    {
      val graph = Generator.generateGraph(t, n, v, sc, partitions)
      CustomLogger.logger.info(s"Config : T = $t / N = $n / V = $v")


      val coloring: Algorithm = {
        if (algo_version == 1)
          new KP_Coloring()
        else if (algo_version == 2)
          new AlgorithmFC2()
        else
          throw new RuntimeException("Wrong version")
      }

        // Looping the algo
        for (i <- 1 to loops) {
          CustomLogger.logger.info(s"Test n $i/$loops")

          //val res = coloring.execute(graph, 1000, sc)
          //coloring.printGraphProper(  res)

          // val result = s"L'algorithme greedy a choisi ${coloring.getBiggestColor(res)} couleurs."
          //CustomLogger.logger.info(result)

          exec_for_gc(coloring, graph, sc)
          System.gc()
        }

      } //fin gros if coloring


      //Nouvel algo
    else if (algo_version == 3)
    {

      val graph = Generator.generate_nodes_and_edges(t, n, v, sc, partitions)
      CustomLogger.logger.info(s"Config : T = $t / N = $n / V = $v")

      // Looping the algo
      for (i <- 1 to loops) {
        CustomLogger.logger.info(s"Test n $i/$loops")
        //val res = coloring.execute(graph, 1000, sc)
        //coloring.printGraphProper(  res)
        // val result = s"L'algorithme greedy a choisi ${coloring.getBiggestColor(res)} couleurs."
        //CustomLogger.logger.info(result)
        exec_for_gc2( vertices = graph._1, edges = graph._2, sc = sc)
        System.gc()
      }

    }


    sc.stop()
  }
}