package org.apache.spark.graphx

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.{SparkConf, SparkContext}

object ColoringProgram {

  def exec_for_gc(coloring : Algorithm, graph: Graph[Models.node, String] , sc : SparkContext): Unit =
  {
    val res = coloring.execute(graph, 1000, sc)
    val result = s"L'algorithme greedy a choisi ${coloring.getBiggestColor(res)} couleurs."
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
          new AlgoColoring()
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



    sc.stop()
  }
}