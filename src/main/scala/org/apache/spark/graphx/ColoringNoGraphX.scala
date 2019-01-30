package org.apache.spark.graphx

/* This algorithm has been optimized to not use shuffles. We use broadcasted arrays instead */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer

case class node_data(knighthood : Int = 0, color : Int = 0, tiebreakvalue : Int = 0)
{
 override def toString: String = s"tiebreakValue : $tiebreakvalue color : $color knighthood : $knighthood"
}
case class edge_data(src : Long, dst : Long)
//I don't plan to join nodes to edge. I just plan to use a broadcast variable.
//The edges do not hav e to be stored in memory.

//todo : les fonctions knight candidate et tiebreaker peuvent etre combinées. Ça fait + de sens ensemble je pense.

class ColoringWithoutGraphX extends Serializable
{
 type node = RDD[Tuple2[Long, node_data]]
 type edge = RDD[edge_data]
 type graph = Tuple2[node, edge]

 //This function generates the tiebreaker messages
 //It returns an Option RDD of messages
 def tieBreakerMessages(myVertices : node, edges : edge, context : SparkContext): Option[RDD[(Long,Long)]] =
 {
  //Broadcast the vertex data, we need it.
  //This is also called a broadcast hash join
  var bv_vdata = context.broadcast( myVertices.collectAsMap())

  //We iterate through all the edges
  var tiebreaker_messages: RDD[(Long, Long)] = edges.flatMap(edge => {

   val msgs = new ArrayBuffer[Tuple2[Long,Long]]()

   //We get the tiebreaker of SRC
   val tbValuesrc = bv_vdata.value(edge.src).tiebreakvalue
   val ksrc = bv_vdata.value(edge.src).knighthood

   //We get the tiebreaker of DST
   val tbValuedst = bv_vdata.value(edge.dst).tiebreakvalue
   val kdst = bv_vdata.value(edge.dst).knighthood

   //If both nodes are peasants, we can send tiebreakers
   //If this is the last iteration, we can send tiebreakers all the time.
   if ( ksrc == 0 && kdst == 0)
   {
    msgs += Tuple2(edge.src, tbValuedst)
    msgs += Tuple2(edge.dst, tbValuesrc)
   }
   else None
  })

  //Program exit condition here
  if (tiebreaker_messages.isEmpty()) return None

  //Else we can continue
  //Find the best tiebreaker for each vertex
  tiebreaker_messages = tiebreaker_messages.reduceByKey( (a,b) => {
   if (a < b) a
   else b
  })

  return Some(tiebreaker_messages)
 }

 def makeKnightCandidates(firstIteration : Boolean, vertices : node, tiebreaker_messages : RDD[(Long,Long)], context : SparkContext): node =
 {

  //Broadcast the tiebreakers to avoid a shuffle
  val bv_tiebreakers = context.broadcast(  tiebreaker_messages.collectAsMap() )
  var myVertices = vertices

  //Make knight candidates
  //If we get a negative tiebreaker, this is a knight
  myVertices = myVertices.map(  elem =>
  {
   val node_tiebreaker = elem._2.tiebreakvalue
   val msg_tiebreaker: Option[Long] = bv_tiebreakers.value.get(elem._1)

   def innerlogic(): (Long, node_data) = {

    //Quick check for a knight or an disconnected vertex (the algorithm handles these as well)
    if (elem._2.knighthood == 2 ||  msg_tiebreaker.isEmpty) return elem

    //Handle first iteration case
    if (firstIteration && node_tiebreaker < msg_tiebreaker.get)
     return (elem._1, new node_data(knighthood = 2, color = 1, elem._2.tiebreakvalue))

    //Handle knight candidate case
    if (node_tiebreaker < msg_tiebreaker.get)
     return (elem._1, new node_data(knighthood = 1, color = elem._2.color, elem._2.tiebreakvalue))

    //This is the case when the vertex stays a peasant
    return elem
   }
   innerlogic()
  })

  //debug tiebreaker_messages
 // println("checking status of knight candidates")
  //myVertices.collect().sortBy(_._1)foreach(println)

  //Return the new array of vertices
  myVertices
 }

 //This function is pretty long and does a lot
 def selectKnightColor(vertices : node, edges : edge, context : SparkContext, lastIteration : Boolean): node = {

  var myVertices = vertices
  //Now we have to make the knight candidates into proper knights.
  //We gather the colors of the adjacent knights.
  //First we need to rebroadcast the vertex structure
  val bv_vdata = context.broadcast( myVertices.collectAsMap())

  //Iterate all edges
  //Edges can stay on the disk at all times
  //We simply iterate through them. mapPartition
  var colormsgs = edges.flatMap(edge => {
   //Message is an INT value (the color)
   //val ret = new ArrayBuffer[Tuple2[Long,Int]]()

   val ksrc = bv_vdata.value(edge.src).knighthood
   val kdst = bv_vdata.value(edge.dst).knighthood

   val srcColor = bv_vdata.value(edge.src).color
   val dstColor = bv_vdata.value(edge.dst).color

   //This selector selects the best color
   def selector() : Option[Tuple2[Long, Int]] = {

    //If this is the last iteration, we send colors from all knights to peasants
    if (lastIteration) {
     if (ksrc == 2 && kdst == 0) {
      return Some(Tuple2(edge.dst, srcColor))
     }
     if (ksrc == 0 && kdst == 2) {
      return Some(Tuple2(edge.src, dstColor))
     }
    }

    //If one node is a knight candidate, and the other is a knight, we can send the color
    if (ksrc == 1 && kdst == 2)
     return Some( Tuple2(edge.src, dstColor ))
    if (ksrc == 2 && kdst == 1)
     return Some(Tuple2(edge.dst, srcColor ))

    return None
   }

   val result = selector()
   result
  })

  //http://codingjunkie.net/spark-agr-by-key/
  val colorList = ArrayBuffer[Int]()
  val addToList = (s: ArrayBuffer[Int], v: Int) => s += v
  val mergeLists = (p1: ArrayBuffer[Int], p2: ArrayBuffer[Int]) => p1 ++= p2

  val colors = colormsgs.aggregateByKey(colorList)(addToList,mergeLists)


  //debug tiebreaker_messages
 // println("printing available colors")
 // colors.collect().sortBy(_._1).foreach(println)

  //Now we select the new color for each vertex
  //We find the first smallest color
  val newColors = colors.map( elem => {

   val colors = elem._2.sorted
   var newColor = 1
   var index = 0

   //Loop function with return acting as a break statement.
   def loop(): Unit = {
    while (true) {
     //Check for empty array, or for end.
     if (index == colors.length || colors.isEmpty) return

     //If color is present in the array, we keep searching for an empty slot
     if (newColor == colors(index)) {
      newColor += 1
      index += 1
     }
     else {
      return
     }
    }
   }
   loop()
   (elem._1, newColor)
  })


  //Get the new colors
  //Use hashmap instead of join
  val bv_newcolors = context.broadcast(newColors.collect().toMap)

  //Map them unto the existing vertices. Also, make them into knights now
  myVertices = myVertices.map(  v =>
  {
   //Get new color if there's one.
   val result: Option[Int] = bv_newcolors.value.get(  v._1)
   if (result.nonEmpty) {
    val newColor = bv_newcolors.value(v._1)
    (v._1,  new node_data( knighthood = 2, color = newColor))
   }
   else v
  })

  myVertices
 }


 def execute(vertices : node, e : edge,  context : SparkContext) :  graph =
 {
  var counter = 0
  var myVertices: node = vertices.cache()

  //First iteration is off loop.
  //We exchange tiebreakers
  counter += 1
  println("Iteration numero : " + counter)
  val msg1 = tieBreakerMessages(myVertices, e, context)
  if (msg1.isEmpty) return (myVertices,e) //condition de sortie ici

  //We make the first knights
  myVertices = makeKnightCandidates( true, myVertices, msg1.get, context)

  //Now we can go into the loop
  //Call the loop
  inner_loop()

  def inner_loop() : Unit =
  {
   while (true)
   {
    //myVertices = myVertices.localCheckpoint()
    // myVertices.collect().sortBy(_._1)foreach(println)

    counter += 1
    println("Iteration numero : " + counter)

    //debug vertices at start of iterations
   // println("debugging graph at the start of the iterations")
  //  myVertices.collect().sortBy(_._1).foreach(println)

    val msg1 = tieBreakerMessages(myVertices, e, context)
    if (msg1.isEmpty) return

    //We select the knight candidates
    myVertices = makeKnightCandidates( false, myVertices, msg1.get, context)

    //We select a color for them
    myVertices = selectKnightColor(myVertices, e, context, false)

   } // while loop

   (myVertices, e)//while loop ends here
  } //dummy function ends here

  //Color the last vertices before end
  //These vertices are isolated and can take their best available color

  //Choose a color here
  myVertices = selectKnightColor( myVertices, e, context, true)

  //Final graph print
 println("Final graph")
 myVertices.collect().sortBy(_._1).foreach(println)

  //Return
  ( myVertices, e)
 }
}

object testPetersenGraph2 extends App {
 val conf = new SparkConf()
   .setAppName("Petersen Graph (10 nodes)")
   .setMaster("local[*]")
 val sc = new SparkContext(conf)
 sc.setLogLevel("ERROR")
 var myVertices = sc.makeRDD(Array(
  //      (1L, new node(id = 1, tiebreakValue = 3)), //A
  //      (2L, new node(id = 2, tiebreakValue = 5)), //B
  //      (3L, new node(id = 3, tiebreakValue = 1)), //C
  //      (4L, new node(id = 4, tiebreakValue = 7)), //D
  //      (5L, new node(id = 5, tiebreakValue = 10)), //E
  //      (6L, new node(id = 6, tiebreakValue = 2)), //F
  //      (7L, new node(id = 7, tiebreakValue = 3)), //G
  //      (8L, new node(id = 8, tiebreakValue = 4)), //H
  //      (9L, new node(id = 9, tiebreakValue = 6)), //I
  //      (10L, new node(id = 10, tiebreakValue = 8)))) //J
  (1L, new node_data(tiebreakvalue = 1)), //A
  (2L, new node_data(tiebreakvalue = 2)), //B
  (3L, new node_data(tiebreakvalue = 3)), //C
  (4L, new node_data(tiebreakvalue = 4)), //D
  (5L, new node_data(tiebreakvalue = 5)), //E
  (6L, new node_data(tiebreakvalue = 6)), //F
  (7L, new node_data(tiebreakvalue = 7)), //G
  (8L, new node_data(tiebreakvalue = 8)), //H
  (9L, new node_data(tiebreakvalue = 9)), //I
  (10L, new node_data(tiebreakvalue = 10)))) //J

 var myEdges: RDD[edge_data] = sc.makeRDD(Array(
  edge_data(1L, 2L), edge_data(1L, 3L), edge_data(1L, 6L),
  edge_data(2L, 7L), edge_data(2L, 8L),
  edge_data(3L, 4L), edge_data(3L, 9L),
  edge_data(4L, 5L), edge_data(4L, 8L),
  edge_data(5L, 6L), edge_data(5L, 7L),
  edge_data(6L, 10L),
  edge_data(7L, 9L),
  edge_data(8L, 10L),
  edge_data(9L, 10L)
 ))

 val coloring = new ColoringWithoutGraphX()
 val res = coloring.execute( myVertices, myEdges, sc)
 //println("\nNombre de couleur trouvées: " + algoColoring.getChromaticNumber(res))
}


object testProblem extends App {

 val conf = new SparkConf()
   .setAppName("test a problem")
   .setMaster("local[*]")
 val sc = new SparkContext(conf)
 sc.setLogLevel("ERROR")

 var myEdges: RDD[edge_data] = sc.makeRDD(Array(
 edge_data(2,1),
 edge_data(3,1),
 edge_data(3,2),
 edge_data(4,1),
 edge_data(4,2),
 edge_data(4,3),
 edge_data(5,2),
 edge_data(5,4),
 edge_data(6,1),
 edge_data(6,3),
 edge_data(6,5),
 edge_data(7,2),
 edge_data(7,4),
 edge_data(7,5),
 edge_data(7,6),
 edge_data(8,1),
 edge_data(8,3),
 edge_data(8,5),
 edge_data(8,6),
 edge_data(8,7),
 edge_data(9,3),
 edge_data(9,4),
 edge_data(9,7),
 edge_data(9,8),
 edge_data(10,1),
 edge_data(10,2),
 edge_data(10,7),
 edge_data(10,8),
 edge_data(10,9),
 edge_data(11,3),
 edge_data(11,4),
 edge_data(11,5),
 edge_data(11,6),
 edge_data(11,9),
 edge_data(11,10),
 edge_data(12,1),
 edge_data(12,2),
 edge_data(12,5),
 edge_data(12,6),
 edge_data(12,9),
 edge_data(12,10),
 edge_data(12,11)
 ))


 var myVertices: RDD[(Long, node_data)] = sc.makeRDD(Array(

  (1L, new node_data(tiebreakvalue = 5)), //A
  (2L, new node_data(tiebreakvalue = 4)), //B
  (3L, new node_data(tiebreakvalue = 7)), //C
  (4L, new node_data(tiebreakvalue = 12)), //D
  (5L, new node_data(tiebreakvalue = 11)), //E
  (6L, new node_data(tiebreakvalue = 8)), //F
  (7L, new node_data(tiebreakvalue = 1)), //G
  (8L, new node_data(tiebreakvalue = 2)), //H
  (9L, new node_data(tiebreakvalue = 9)), //I
  (10L, new node_data(tiebreakvalue = 6)), //J
 (11L, new node_data(tiebreakvalue = 3)), //I
 (12L, new node_data(tiebreakvalue = 10)))) //I


 val coloring = new ColoringWithoutGraphX()
 val res = coloring.execute( myVertices, myEdges, sc)

}
