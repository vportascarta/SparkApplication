package org.apache.spark.graphx

/* This algorithm has been optimized to not use shuffles. We use broadcasted arrays instead */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.SortedSet

case class node_matrix(tiebreakvalue : Int = 0, len : Int = 20)
{
  var changed : Byte = 0 //0 = didnt change ,1 = changed
var knighthood : Byte = 0
  var color = 0
  val adjvector = new Array[Byte](len)

  override def toString: String = {
    var output = ""
    output += s"tiebreakValue : $tiebreakvalue color : $color knighthood : $knighthood  changed:$changed\n"

     for (i <- adjvector)
       output += s"$i "

    output
  }
}
case class edge_data(src : Long, dst : Long)
//I don't plan to join nodes to edge. I just plan to use a broadcast variable.
//The edges do not hav e to be stored in memory.

//todo : les fonctions knight candidate et tiebreaker peuvent etre combinées. Ça fait + de sens ensemble je pense.

class ColoringMatrix extends Serializable {
  var debug = true
  type node = RDD[Tuple2[Long, node_matrix]]

  //We use accumulators to detect the end of iterations. Otherwise we need a "changed" value on every node.
  def makeKnights(firstIteration: Boolean, vertices: node, context: SparkContext,
                  acc : LongAccumulator,
                  vertices_bcast: Broadcast[Array[(Long, node_matrix)]]): node =
  {

    //Make knight candidates
    //This is also the return value
    vertices.map(v => {
      //We will return a modified node object
      var returnValue: (Long, node_matrix) = v

      //First we check to see if we can become a knight
      val src = v._1.toInt - 1 //get the src
      val adj = v._2.adjvector
      val src_tb = v._2.tiebreakvalue

      //Is this node already a knight? Let's check.
      //If the node is already a knight, we don't do any work.
      val src_knight = v._2.knighthood

      def trybecomeKnight() : (Long, node_matrix) =
      {
        var becomeKnight = false
        //var listColors = scala.collection.mutable.Set[Int]()
        var listColors = new Array[Byte](9000)

        go_adjlist()
        def go_adjlist() : Unit =
        {
          for (i <- 0 until adj.length)
          {
            if (adj(i) == 1) //if adj
            {
              val dst = i.toInt
              val dst_tb = vertices_bcast.value(dst)._2.tiebreakvalue
              val dst_knight = vertices_bcast.value(dst)._2.knighthood
              //If our tb value is beaten, we can't become a knight. We can only be beaten by peasants.
              if (src_tb > dst_tb && dst_knight == 0) {
                becomeKnight = false
                return
              }

              //If the neighbor is a knight, we save his color. We also get the color when its the first iteration.
              if (dst_knight == 2 || firstIteration == true)
                listColors(vertices_bcast.value(dst)._2.color) = 1
              //listColors += vertices_bcast.value(dst)._2.color
            }
          } //end for
          becomeKnight = true
        } // end of function go adjlist

        var color = 0
        //If we are to become a knight, we can select our best color right away
        if (becomeKnight == true)
        {
          //Add to the accumulator. This is needed for our exit condition.
          acc.add(1)
          color = findcolor()

          def findcolor(): Int = {
            for (i <- 1 to listColors.length) {
              if (listColors(i) == 0) //if this color is not present
                return i
            }
            return listColors.length
          }
        }
        //Now that we have chosen a color, we can overwrite this vertex
        returnValue._2.changed = 0
        if (becomeKnight == true)
        {
          returnValue._2.knighthood = 2 //proper knight now
          returnValue._2.color = color //with a color
          returnValue._2.changed = 1
        }
        returnValue
      }

      //If this node is a knight. We return the same knight node (no change)
      if (src_knight == 2)
      {
        returnValue._2.changed = 0
      }
      //Else, we try to make it into a knight.
      else {
        returnValue = trybecomeKnight()
      }

      returnValue

    })
  }  //make knights

  def execute(vertices : node,  context : SparkContext) : node =
  {
    var counter = 0
    var myVertices = vertices
    myVertices.cache()

    //Accumulator
    var acc: LongAccumulator = context.longAccumulator("number of changed graph nodes")
    acc.reset()

    //Broadcast the vertices structure
    //1ere action ici
    var vertices_bcast: Broadcast[Array[(Long, node_matrix)]] = context.broadcast(myVertices.collect())

    println("Printing initial graph")
    myVertices.collect() foreach println
    myVertices = makeKnights(true, myVertices, context, acc, vertices_bcast )

    val rien = myVertices.take(1)
    var initialAcc = acc.value
    println("Valeur initiale de l'accumulateur : " + initialAcc)
    acc.reset()

    initialAcc = acc.value
    println("Valeur apres reset de l'accumulateur : " + initialAcc)

    if (debug) {
      println("Printing first knights")
      myVertices.collect().filter( e => {
        if (e._2.changed == 1) true
        else false
      }
      )sortBy(_._1) foreach(println)
    }

    //Now we can go into the loop
    //Call the loop
    inner_loop()

    def inner_loop() : Unit =
    {
      while (true)
      {
        acc.reset()
        counter += 1

        //Update the broadcasted structure
        vertices_bcast = context.broadcast(myVertices.collect())

        println("Iteration numero : " + counter)
        myVertices = makeKnights(false, myVertices, context, acc, vertices_bcast)
        val remaining = myVertices.filter(_._2.changed == 1)
        if (remaining.count() == 0) return

        if (debug) {
          println("Printing new knights")
          myVertices.collect().filter( e => {
            if (e._2.changed == 1) true
            else false
          }
          )sortBy(_._1)foreach(println)
        }

        println("VALEUR DE L'ACCMULATEUR : " + acc.value)

      } // while loop
      (myVertices)//while loop ends here
    } //dummy function ends here

    //Color the last vertices before end
    //These vertices are isolated and can take their best available color

    //Final graph print

    if (debug) {
      println("Final graph")
      myVertices.collect().sortBy(_._1).foreach(println)
    }

    //Return
    ( myVertices)
  }
}


object testPetersenAdjlist extends App {

  val conf = new SparkConf()
    .setAppName("Petersen Graph ADJVECTOR (10 nodes)")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  var vertices = Array(
    (1L, new node_matrix(tiebreakvalue = 1)), //A
    (2L, new node_matrix(tiebreakvalue = 2)), //B
    (3L, new node_matrix(tiebreakvalue = 3)), //C
    (4L, new node_matrix(tiebreakvalue = 4)), //D
    (5L, new node_matrix(tiebreakvalue = 5)), //E
    (6L, new node_matrix(tiebreakvalue = 6)), //F
    (7L, new node_matrix(tiebreakvalue = 7)), //G
    (8L, new node_matrix(tiebreakvalue = 8)), //H
    (9L, new node_matrix(tiebreakvalue = 9)), //I
    (10L, new node_matrix(tiebreakvalue = 10))) //J

  var edges = Array(
    edge_data(1L, 2L), edge_data(1L, 3L), edge_data(1L, 6L),
    edge_data(2L, 7L), edge_data(2L, 8L),
    edge_data(3L, 4L), edge_data(3L, 9L),
    edge_data(4L, 5L), edge_data(4L, 8L),
    edge_data(5L, 6L), edge_data(5L, 7L),
    edge_data(6L, 10L),
    edge_data(7L, 9L),
    edge_data(8L, 10L),
    edge_data(9L, 10L)
  )

  //Fill the adjmatrix
  edges.foreach( e => {
    val src = e.src.toInt - 1
    val dst = e.dst.toInt - 1
    vertices( src)._2.adjvector(dst) = 1
    vertices( dst)._2.adjvector(src) = 1
  })

  vertices foreach println

  val algo = new ColoringMatrix()
  algo.execute( sc.makeRDD(vertices), sc)

}