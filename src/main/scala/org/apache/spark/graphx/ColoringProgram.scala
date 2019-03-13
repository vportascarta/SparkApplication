package org.apache.spark.graphx

import ca.lif.sparklauncher.app.CustomLogger
import org.apache.spark.graphx.ColoringProgram.exec_for_gc2
import org.apache.spark.graphx.Generator.generate_graph_matrix
import org.apache.spark.graphx.Models.node
import org.apache.spark.graphx.runLotsOfTests.numberOfloops
import org.apache.spark.graphx.test.{calculateChromaticNumber, numColors, result}
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


  def exec_for_gc2(vertices: Array[(Long, node_data)], edges: Vector[edge_data], sc : SparkContext): Int =
  {
    val algo = new BCastColoring()
    //Generate tiebreakers
    var myVertices = random_tiebreakers(vertices)
    val res: (algo.node, algo.edge) = algo.execute( vertices = sc.makeRDD(myVertices), e = sc.makeRDD(edges), sc)
    val numColors = getBiggestColor_2(res._1.collect())
    //val result = s"L'algorithme greedy a choisi ${numColors} couleurs."
    //println(result)
    //CustomLogger.logger.info(result)

    numColors
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
        CustomLogger.logger.info(s"Algorithm : Knights and Peasants with Hash-broadcast joins")

        val numVertices = graph._1.size
        val numEdges = graph._2.size

        CustomLogger.logger.info(s"Size of problem : $numVertices vertices and $numEdges edges")

        //val res = coloring.execute(graph, 1000, sc)
        //coloring.printGraphProper(  res)
        // val result = s"L'algorithme greedy a choisi ${coloring.getBiggestColor(res)} couleurs."
        //CustomLogger.logger.info(result)

        val t1 = System.nanoTime()

//        exec_for_gc2( vertices = graph._1, edges = graph._2, sc = sc)

        val t2 = System.nanoTime()
        val time_elapsed =  (t2 - t1).toDouble  / 1000000000

        CustomLogger.logger.info(s"Time elapsed : $time_elapsed seconds")

        System.gc()
      }

    }

    sc.stop()
  }
}


object runLotsOfTests extends App
{

  val conf = new SparkConf()
    .setAppName("Test everything and write results")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  /* Syntax is :

  2;3;2;COLORING_SPARK;2.559;4;
  t n v
   */

  // PrintWriter
  //ouvrir en mode append
  import java.io._
  val pw = new PrintWriter(new FileOutputStream("results_coloring.txt", true))
 // pw.write("Hello, world")

  val numberOfloops = 10

  //T
  for (t <- 2 to 5)
    {
      //vary n
        for (n <- 2 to 10 )
        {
          //vary v
          for (v <- 2 to 4 )
          {
              gen(t,n,v)
          }
        }
    }

  def gen(t : Int, n : Int, v : Int): Unit =
  {

    //Check for invalid graphs
    if ((t >= n) || (t >= v)) return

    val graph = Generator.generate_nodes_and_edges(t, n, v, sc, 12)

    val numVertices = graph._1.size
    val numEdges = graph._2.size


    for (i <- 0 until numberOfloops)
      {
        println(s"Config : T = $t / N = $n / V = $v")
        println(s"Algorithm : Knights and Peasants with Hash-broadcast joins")
        println(s"Test n $i/$numberOfloops")
        println(s"Size of problem : $numVertices vertices and $numEdges edges")

        val t1 = System.nanoTime()

        val numcolors = exec_for_gc2(graph._1, graph._2, sc)
        println(s"L'algorithme greedy a choisi ${numcolors} couleurs")

        val t2 = System.nanoTime()
        val time_elapsed =  (t2 - t1).toDouble  / 1000000000

        println(s"Time elapsed : $time_elapsed seconds")

        //Write the results to our file
        pw.write(s"$t;$n;$v;COLORING_BROADCAST;$time_elapsed;$numcolors\n")
        pw.flush()

        System.gc()

      }

  }

  //Close file
  pw.close

}



object runColoringTests
{


  //Generate random tiebreakers for an array of vertices
  def random_tiebreakers(v : Array[node_matrix] ): Array[node_matrix] =
  {

    val count = v.size
    var ids_random: ArrayBuffer[Int] = new ArrayBuffer()
    Random.shuffle(1 to count).copyToBuffer(ids_random)

    var cc = 0

    for (i <- 0 until v.length)
      v(i).tiebreakvalue = ids_random(i)

    v
  }


  def calculateChromaticNumber(v : Array[node_matrix]): Int =
  {
    var biggestColor = 0
    v.foreach(  elem => {
      if (elem.color > biggestColor)
        biggestColor = elem.color
    })

    biggestColor

  }


  def run(params : scala.collection.mutable.Map[String,String]):  Unit =
  {

    val conf = new SparkConf()
      .setAppName("Graph Coloring calculations")
      .setMaster("local[*]")
      .set("spark.local.dir", "/media/data/") //The 4TB hard drive can be used for shuffle files
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

    // PrintWriter
    //ouvrir en mode append
    import java.io._
    val pw = new PrintWriter(new FileOutputStream("results_tspark.txt", true))

    var numberOfloops = 1

    //Initial should be 2 everywhere
    var initialT = 2
    var initialN = 3
    var initialV = 2

    var maxT = 2
    var maxN = 3
    var maxV = 2

    var print = "false"

    if (params != null) {
      numberOfloops = params("loops").toInt
      initialT = params("t").toInt
      initialN = params("n").toInt
      initialV = params("v").toInt
      maxT = params("tMax").toInt
      maxN = params("nMax").toInt
      maxV = params("vMax").toInt
      print = params("print")
    }

    //T
    for (t <- initialT to maxT)
    {
      //vary n
      for (n <- initialN to maxN )
      {
        //vary v
        for (v <- initialV to maxV )
        {
          gen(t,n,v)
        }
      }
    }

    def gen(t : Int, n : Int, v : Int) =
    {

      println("Generating graph in dotfile format...")
      val t1gen = System.nanoTime()
      var graph = generate_graph_matrix(t,n,v)
      val t2gen = System.nanoTime()
      val time_elapsed =  (t2gen - t1gen).toDouble  / 1000000000
      println(s"Time elapsed for generation: $time_elapsed seconds")


      val algorithm = new ColoringMatrix()

      for (i <- 0 until numberOfloops)
      {
        println(s"Config : T = $t / N = $n / V = $v")
        println(s"Algorithm : Graph Coloring Knights & Peasants with Matrix")
        println(s"Test n $i/$numberOfloops")

        graph = random_tiebreakers(graph)

        val t1 = System.nanoTime()
        val result = algorithm.execute(sc.makeRDD(graph), sc)
        val t2 = System.nanoTime()

        val numColors = calculateChromaticNumber(result.collect())
        println(s"Number of colors used to color the graph : $numColors")
        val time_elapsed =  (t2 - t1).toDouble  / 1000000000
        println(s"Time elapsed : $time_elapsed seconds")

        if (print == "true")
          result.collect.foreach(println)

        //Write the results to our file
        pw.append(s"$t;$n;$v;KPCOLORING_MATRIX;$time_elapsed;$numColors\n")
        pw.flush()
        System.gc()

      }

    }

    //Close file
    pw.close


  }


}






