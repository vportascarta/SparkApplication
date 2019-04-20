//Edmond La Chance UQAC 2019  Knights and Peasants algorithm with adj vectors
package org.apache.spark.graphx
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

case class node_matrix(tb : Int = 0, len : Int = 20)
{
  var tiebreakvalue = tb
  var changed : Byte = 0 //0 = pas de changement , 1= changé
var knighthood : Byte = 0
  var color = 0
  val adjvector = new Array[Byte](len)

  override def toString: String = {
    var output = ""
    output += s"tiebreakValue : $tiebreakvalue color : $color knighthood : $knighthood  changed:$changed"
    output
  }
}
case class compact_node(tiebreakvalue : Int, color : Int, knighthood : Byte)
case class edge_data(src : Long, dst : Long)

class KP_ADJVECTOR extends Serializable
{
  var debug = false
  var problemSize : Int = 0
  type node = RDD[Tuple2[Long, node_matrix]]

  def trybecomeKnight(vertices_bcast : Broadcast[Array[(VertexId, compact_node)]],
                      adj : Array[Byte], src_tb : Int)
  : Int =
  {
    var becomeKnight = false
    var listColors = new Array[Byte](problemSize) //We set the length of this array to the worst case.

    go_adjlist()
    def go_adjlist() : Unit =
    {
      for (i <- 0 until adj.length)
      {
        if (adj(i) == 1)
        {
          val dst = i.toInt
          val dst_tb = vertices_bcast.value(dst)._2.tiebreakvalue
          val dst_knight = vertices_bcast.value(dst)._2.knighthood
          //If our tb value is beaten, we can't become a knight. We can only be beaten by peasants.
          if (src_tb > dst_tb && dst_knight == 0) {
            becomeKnight = false
            return
          }

          //If the neighbor is a knight, we save his color.
          if (dst_knight == 2)
            listColors(vertices_bcast.value(dst)._2.color) = 1
        }
      } //end for
      becomeKnight = true
    } // end of function go adjlist

    var color = 0

    //If we are to become a knight, we can select our best color right away
    if (becomeKnight == true)
    {
      color = findcolor()

      def findcolor(): Int = {
        for (i <- 1 to listColors.length) {
          if (listColors(i) == 0) //if this color is not present
            return i //we choose it
        }
        return 1 //Special case : No knights are present. Happens at the first iteration and for isolated vertices.
      }
    }
    //Return the color
    color
  }


  def execute(vertices : node,  context : SparkContext) : node =
  {
    problemSize = vertices.take(1)(0)._2.adjvector.length
    var counter = 0
    var myVertices = vertices

    //Now we can go into the loop
    inner_loop()
    def inner_loop() : Unit =
    {
      while (true)
      {

        counter += 1
        //We broadcast only the necessary data
        val compact = myVertices.map( v => {
          (v._1,  new compact_node(v._2.tiebreakvalue, v._2.color, v._2.knighthood))
        })

        var vertices_bcast = context.broadcast(compact.collect())
        compact.unpersist(true)

        println("Iteration numero : " + counter)
        myVertices =  myVertices.map(v =>
           {
             //We will return a modified node object
             var returnValue: (VertexId, node_matrix) = v
             returnValue._2.changed = 0 //If this node does not become a knight, we still have to do this.

             //First we check to see if we can become a knight
             val src = v._1.toInt //get the src
           val adj: Array[Byte] = v._2.adjvector
             val src_tb: Int = v._2.tiebreakvalue
             val src_knight = v._2.knighthood

             //If the node is a peasant, we do some work to see if we can become a knight
             if (src_knight != 2)
             {
               val color : Int = trybecomeKnight(vertices_bcast, adj, src_tb)
               //If the function returns a color > 0, this vertex becomes a knight
               //Else, we stay a peasant
               if (color > 0)
               {
                 returnValue._2.color = color
                 returnValue._2.changed = 1
                 returnValue._2.knighthood = 2
               }
             }
             returnValue
           }).cache()

        myVertices = myVertices.localCheckpoint()

        if (myVertices.filter(_._2.changed == 1).count() == 0) return

//        if (debug == true) {
//          println("Printing the new knights :")
//          newKnights.foreach(println)
//        }

        vertices_bcast.unpersist(true)

//        knightsRDD.unpersist(true)
//        newKnights.unpersist(true)
//        compact.unpersist(true)
        //if (newKnights.collect().size == 0) return
      //  myVertices.unpersist(true)
        //compact.unpersist(true)
       // newKnights.unpersist(true)

      } // while loop
      (myVertices)//while loop ends here
    } //dummy function ends here

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
    .setAppName("Petersen Graph version Examen")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  var edges = Array(
    edge_data(1L, 2L), edge_data(1L, 3L), edge_data(1L, 6L),
    edge_data(2L, 7L), edge_data(2L, 8L),
    edge_data(3L, 4L), edge_data(3L, 9L),
    edge_data(4L, 5L), edge_data(4L, 8L),
    edge_data(5L, 6L), edge_data(5L, 7L),
    edge_data(6L, 10L),
    edge_data(7L, 9L),
    edge_data(8L, 10L),
    edge_data(9L, 10L),
    edge_data(10L, 11L),
    edge_data(9L, 11L),
    edge_data(7L, 11L),
    edge_data(8L, 11L)
  )
  var vertices = Array(
    (0L, new node_matrix()), //dummy isolated graph node to make everything work without a hash table
    (1L, new node_matrix(tb = 6, len = 12)),
    (2L, new node_matrix(tb = 11, len = 12)),
    (3L, new node_matrix(tb = 10, len = 12)),
    (4L, new node_matrix(tb = 1, len = 12)),
    (5L, new node_matrix(tb = 2, len = 12)),
    (6L, new node_matrix(tb = 7, len = 12)),
    (7L, new node_matrix(tb = 4, len = 12)),
    (8L, new node_matrix(tb = 9, len = 12)),
    (9L, new node_matrix(tb = 8, len = 12)),
    (10L, new node_matrix(tb = 3, len = 12)),
    (11L, new node_matrix(tb = 5, len = 12)))

  //Fill the adjmatrix
  edges.foreach( e => {
    val src = e.src.toInt
    val dst = e.dst.toInt
    vertices( src)._2.adjvector(dst) = 1
    vertices( dst)._2.adjvector(src) = 1
  })
  vertices foreach println

  val algo = new KP_ADJVECTOR()
  algo.execute( sc.makeRDD(vertices), sc)

}