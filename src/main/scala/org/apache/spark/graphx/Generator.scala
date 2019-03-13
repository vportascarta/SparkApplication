package org.apache.spark.graphx

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.util.{ArrayList, List}

import ca.lif.sparklauncher.app.CustomLogger
import ca.uqac.lif.testing.tway._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Generator.generate_graph_matrix
import org.apache.spark.graphx.Models.node
import org.apache.spark.rdd.RDD

import scala.collection.mutable


object Generator {
  def generateGraph(t: Int, n: Int, v: Int, sc: SparkContext, partitions: Int): Graph[node, String] = {
    CustomLogger.logger.info("BEGIN GENERATION")

    val domains = FrontEnd.createDomains(n, v)
    val var_names: List[String] = new ArrayList[String](domains.size)
    var_names.addAll(domains.keySet)

    val twp = new DotGraphGenerator(t, var_names)

    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true, "utf-8")

    twp.setOutput(ps)
    twp.addDomain(domains)
    twp.generateTWayEdges()

    val content = new String(baos.toByteArray, StandardCharsets.UTF_8)

    CustomLogger.logger.info("GENERATION COMPLETE")
    Parser.readGraphVizString(content, sc, partitions)
  }


  def generate_nodes_and_edges( t: Int, n: Int, v: Int, sc: SparkContext, partitions: Int  ) :
      Tuple2[Array[(Long, node_data)],Vector[edge_data]] = {

    CustomLogger.logger.info("BEGIN GENERATION")

    val domains = FrontEnd.createDomains(n, v)
    val var_names: List[String] = new ArrayList[String](domains.size)
    var_names.addAll(domains.keySet)

    val twp = new DotGraphGenerator(t, var_names)

    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true, "utf-8")

    twp.setOutput(ps)
    twp.addDomain(domains)
    twp.generateTWayEdges()

    val content = new String(baos.toByteArray, StandardCharsets.UTF_8)

    CustomLogger.logger.info("GENERATION COMPLETE")

    //Il faut mmaintenant parser "content"

    var edgesVector: Vector[edge_data] = Vector.empty[edge_data]
    //Comment inferer le return??
    def treatLine(line: String): Option[edge_data] = {
      //les sommets commencent a 1
      val values = line.split(" ") //Split with space separator

      //Ligne genre graph ou commentaire
      if (values(0)(0) > '9' || values(0)(0) < '0') {
        return Option(null)
      }

      //Gerer le label (pas besoin)
      if (values(1)(0) == '[') return Option(null)

      //Faire une edge
      val src = values(0).toLong + 1
      val dst = values(2).replace(";", "").toLong + 1

      val e = edge_data(src, dst)
      Option(e)

    }
    //For all the lines in the file
    for (line <- content.split(System.getProperty("line.separator"))) {
      val e = treatLine(line)
      if (e.nonEmpty)
        edgesVector = edgesVector :+ e.get
    }

    var vertices: mutable.Map[VertexId, node_data] = collection.mutable.Map[Long, node_data]()

    //Fill the map
    edgesVector.foreach( e => {
      if (!vertices.isDefinedAt(e.src))
        vertices(e.src) = node_data()

      if (!vertices.isDefinedAt(e.dst))
        vertices(e.dst) = node_data()
    })


   // println("Print le resultat de la generation")
    //println("Les edges : ")
   // edgesVector.foreach(println)

    //println("La map")
    //vertices.toArray.sortBy(_._1).foreach( println)


    (vertices.toArray, edgesVector)

  }


  def generate_graph_matrix( t: Int, n: Int, v: Int) :
  Array[node_matrix] = {

    val domains = FrontEnd.createDomains(n, v)
    val var_names: List[String] = new ArrayList[String](domains.size)
    var_names.addAll(domains.keySet)
    val twp = new DotGraphGenerator(t, var_names)

    val baos = new ByteArrayOutputStream()
    val ps = new PrintStream(baos, true, "utf-8")

    twp.setOutput(ps)
    twp.addDomain(domains)
    twp.generateTWayEdges()

    val content = new String(baos.toByteArray, StandardCharsets.UTF_8)
    println("input graph is  : ")
    println(content)

    //Il faut maintenant parser "content"

    //1. Keep the existing code. Generate edge objects from the string
    //2. As we generate edge objects, we keep the biggest vertex number. This will be used afterwards

    var biggestVertex = 0

    var edgesVector: Vector[edge_data] = Vector.empty[edge_data]
    //Comment inferer le return??
    def treatLine(line: String): Option[edge_data] = {
      //les sommets commencent a 1
      val values = line.split(" ") //Split with space separator

      //Ligne genre graph ou commentaire
      if (values(0)(0) > '9' || values(0)(0) < '0') {
        return Option(null)
      }

      //Gerer le label (pas besoin)
      if (values(1)(0) == '[') return Option(null)

      //Faire une edge
      val src = values(0).toLong + 1
      val dst = values(2).replace(";", "").toLong + 1

      //Max of 2^32 elements as of now
      if (src > biggestVertex) biggestVertex = src.toInt
      if (dst > biggestVertex) biggestVertex = dst.toInt

      val e = edge_data(src, dst)
      Option(e)

    }
    //For all the lines in the file
    for (line <- content.split(System.getProperty("line.separator"))) {
      val e = treatLine(line)
      if (e.nonEmpty)
        edgesVector = edgesVector :+ e.get
    }

    //Now we create the array of node objects.
    val tototo: Array[node_matrix] = new Array[node_matrix](biggestVertex+1)
    for (i <- 0 to biggestVertex) {
        tototo(i) =  node_matrix(0, biggestVertex+1)
    }

    //We fill it using our vector
    for (i <- edgesVector) {
      val src = i.src.toInt
      val dst = i.dst.toInt
      tototo(src).adjvector(dst) = 1
      tototo(dst).adjvector(src) = 1
    }

    //We return the completed data structure.
    tototo
  }

} //fin object generator


object test extends App {

  val res = generate_graph_matrix(2,3,2)
  res.foreach( println)
}

//    var vertices: mutable.Map[VertexId, node_data] = collection.mutable.Map[Long, node_data]()
//
//    //Fill the map
//    edgesVector.foreach( e => {
//      if (!vertices.isDefinedAt(e.src))
//        vertices(e.src) = node_data()
//
//      if (!vertices.isDefinedAt(e.dst))
//        vertices(e.dst) = node_data()
//    })


// println("Print le resultat de la generation")
//println("Les edges : ")
// edgesVector.foreach(println)

//println("La map")
//vertices.toArray.sortBy(_._1).foreach( println)


//(vertices.toArray, edgesVector)