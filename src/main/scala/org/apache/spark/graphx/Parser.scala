package org.apache.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.graphx.Models.Node
import org.apache.spark.storage.StorageLevel

import scala.io.Source

//Helper functions inside this
object Parser {
  //Lire le graphe graphviz
  //  graph G {
  //    0 [label="p1 = 1 p2 = 1 p3 = 1 "];
  //    0 -- 1;
  val defaultStorageLevelVertex: StorageLevel = StorageLevels.MEMORY_AND_DISK_SER
  val defaultStorageLevelEdges: StorageLevel = StorageLevels.MEMORY_AND_DISK_SER

  def readGraphVizString(data: String, sc: SparkContext, partitions: Int, slVertex: StorageLevel = defaultStorageLevelVertex,
                         slEdges: StorageLevel = defaultStorageLevelEdges): Graph[Node, String] = {
    var edgesVector: Vector[Edge[String]] = Vector.empty[Edge[String]]

    //Comment inferer le return??
    def treatLine(line: String): Option[Edge[String]] = {
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

      val e: Edge[String] = Edge(src.toLong, dst, line)
      Option(e)

    }
    //For all the lines in the file
    for (line <- data.split("\n")) {
      val e = treatLine(line)
      if (e.nonEmpty)
        edgesVector = edgesVector :+ e.get
    }

    val erdd = sc.makeRDD(edgesVector, partitions)
    val graph: Graph[Node, String] = Graph.fromEdges(erdd, Node(), slVertex, slEdges).cache()

    graph
  }

  def readGraphVizFile(filename: String, sc: SparkContext, partitions: Int, slVertex: StorageLevel = defaultStorageLevelVertex,
                       slEdges: StorageLevel = defaultStorageLevelEdges): Graph[Node, String] = {
    var edgesVector: Vector[Edge[String]] = Vector.empty[Edge[String]]

    //Comment inferer le return??
    def treatLine(line: String): Option[Edge[String]] = {
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

      val e: Edge[String] = Edge(src.toLong, dst, line)
      Option(e)

    }
    //For all the lines in the file
    for (line <- Source.fromFile(filename).getLines()) {
      val e = treatLine(line)
      if (e.nonEmpty)
        edgesVector = edgesVector :+ e.get
    }

    val erdd = sc.makeRDD(edgesVector, partitions)
    val graph: Graph[Node, String] = Graph.fromEdges(erdd, Node(), slVertex, slEdges).cache()

    graph
  }

  //Lire le graphe
  def readGraphFile(filename: String, sc: SparkContext, partitions: Int): Graph[Node, String] = {


    var edgesVector: Vector[Edge[String]] = Vector.empty[Edge[String]]

    def getNumberVertex: Int = {
      //lookup table
      //Aller chercher le nombre de nodes avant pour la lookup table
      for (line <- Source.fromFile(filename).getLines()) {
        //p edge 25 320
        //0  1   2  3
        if (line(0) == 'p') {
          val values = line.split(" ")
          return values(2).toInt
        }
      }
      0
    }

    var max = getNumberVertex
    max += 1
    val lookup_table: Array[Array[Int]] = Array.fill(max, max)(5)

    //Comment inferer le return??
    def treatLine(line: String): Edge[String] = {
      //e 980 940
      //les sommets commencent a 1
      val values = line.split(" ") //Split with space separator
      val src = values(1)
      val dest = values(2)

      //Si l'arete existe deja, on retourne une arete vide (Graphe non dirige svp!!)
      if (lookup_table(src.toInt)(dest.toInt) == 1) {
        val e: Edge[String] = Edge(0L, 0L, "error") //Return the 0,0 edge
        return e
      }

      //On ajoute dans la matrice d'adjacence
      lookup_table(src.toInt)(dest.toInt) = 1
      lookup_table(dest.toInt)(src.toInt) = 1

      val e: Edge[String] = Edge(src.toLong, dest.toLong, line)
      e
    }
    //For all the lines in the file
    for (line <- Source.fromFile(filename).getLines()) {
      //Construire une liste de edges

      if (line != "") {
        if (line(0) == 'e') {
          val edge = treatLine(line)
          //Check for errors
          if (!(edge.attr == "error")) {
            edgesVector = edgesVector :+ edge
          }
          else {
            //println("DUPLICATE EDGE : " + line)
          }
        }
      }

    }
    val erdd = sc.makeRDD(edgesVector, partitions)
    val graph: Graph[Node, String] = Graph.fromEdges(erdd, Node()) //set all vertices to the color 1, initially

    graph
  }
}