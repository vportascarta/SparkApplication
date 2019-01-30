package org.apache.spark.graphx

//Petit programme exemple pour 8inf803
//Edmond La Chance

/*
Ce petit programme en scala execute un algorithme de graphe itératif sur Spark GraphX. Cet algorithme
essaie de colorier un graphe avec un nombre de couleurs minimal (mais l'algorithme est très random et ne
donne pas de très bons résultats!). Par contre le code est très court donc il est intéressant comme exemple.

Voici comment ce programme fonctionne :

1. L'exécution commence à testPetersenGraph.
2. On crée le graphe directement dans le code. Le tiebreaking value est une valeur random (ici hardcodée)
qui permet a l'algorithme glouton de coloriage de graphe de trancher dans ses décisions
3. La boucle itérative se trouve dans la fonction execute
4. L'algorithme FC2 fonctionne de la façon suivante :
  Chaque itération, les noeuds du graphe s'envoient des messages. Si on est un noeud, et qu'on trouve un voisin qui a un meilleur
  tiebreak, on doit augmenter notre couleur. Les noeuds qui n'augmentent pas gardent une couleur fixe et
  arrêtent d'envoyer des messages.
  L'algorithme s'arrête lorsqu'il n'y a plus de messages envoyés

 C'est donc un algorithme très simple de coloriage (pas le meilleur).
 */

  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.graphx.Models.node
  import org.apache.spark.graphx.testPetersenGraph.{myEdges, myVertices, sc}
  import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}
  import org.apache.spark.rdd.RDD

  import scala.collection.mutable
  import scala.collection.mutable.ArrayBuffer
  import scala.collection.mutable.SortedSet

  //tiebreak value is generated randomly before every graph iteration
//  class node(val id: Long, val color: Int = 1, val knighthood: Int = 0, val tiebreakvalue: Long = 1L) extends Serializable {
//    override def toString: String = s"id : $id tiebreakvalue : $tiebreakvalue color : $color knighthood : $knighthood"
//  }


  class AlgoColoring extends Algorithm {

    var TEST_OR_NOT = "NO"


    def getChromaticNumber(g: Graph[node, String]): Int = {
      val aa = g.vertices.collect()
      var maxColor = 0
      for (i <- aa) {
        if (i._2.color > maxColor) maxColor = i._2.color
      }
      maxColor
    }

    def sendtiebreakvalues(ctx: EdgeContext[node, String, Long]): Unit = {
      if (ctx.srcAttr.knighthood == 0 && ctx.dstAttr.knighthood == 0) {
        ctx.sendToDst(ctx.srcAttr.tiebreakvalue)
        ctx.sendToSrc(ctx.dstAttr.tiebreakvalue)
      }
    }

    def selectBest(id1: Long, id2: Long): Long = {
      if (id1 < id2) id1
      else id2
    }

    def markKnight (vid: VertexId, sommet: node, bestTieBreak: Long): node = {
      if (sommet.tiebreakvalue < bestTieBreak)
        return new node(sommet.id, sommet.color, knighthood = 1, sommet.tiebreakvalue)
      else {
        sommet
      }
    }

    //We go through all the edges.
    //When an edge connects a knight to a knight candidate, we send the color
    def sendKnightColors(ctx : EdgeContext[node, String, SortedSet[Int]]) : Unit =
    {
      val srcColor = ctx.srcAttr.color
      val dstColor = ctx.dstAttr.color

      //SRC is knight candidate and DST is knight
      if (ctx.srcAttr.knighthood == 1 && ctx.dstAttr.knighthood == 2) {

        val t =  SortedSet[Int](dstColor)
        ctx.sendToSrc( t)
      }

      //DST is knight candidate and SRC is knight
      if (ctx.dstAttr.knighthood == 1 && ctx.srcAttr.knighthood == 2) {
        val v = SortedSet[Int](srcColor)
        ctx.sendToDst(  v)
      }

    }

    //Send final colors for the last knights
    def sendFinalColors(ctx : EdgeContext[node, String, SortedSet[Int]]) : Unit =
    {
      val srcColor = ctx.srcAttr.color
      val dstColor = ctx.dstAttr.color

      if (ctx.srcAttr.knighthood == 0) {
        val t = SortedSet(dstColor)
        ctx.sendToSrc(t)
      }

      if (ctx.dstAttr.knighthood == 0) {
        val t = SortedSet(srcColor)
        ctx.sendToDst(t)
      }
    }


    //We merge two colors vectors together. We just want to have our colors in order, so that selecting the lowest possible color is very fast.
    //https://en.wikipedia.org/wiki/Merge_sort (parallel)
    //O(n)
    def mergeColors(a: SortedSet[Int], b: SortedSet[Int]): SortedSet[Int] =
    {
        a union b
    }

    //Become a knight
    def becomeKnight(myid: Long, myNode: node, colors: SortedSet[Int]): node =
    {

      var counter = 1
      var bestColor = 0

      def looop() : Unit =  {
        for (i <- colors) {

          if (i != counter) {
            bestColor = i
            return
          }
          counter += 1
          bestColor = i+1
        }
      } //function end

      looop()


      var newNode = new node(id = myNode.id, knighthood = 2, color = bestColor, tiebreakvalue = myNode.tiebreakvalue)
      newNode
    }


    def execute(g: Graph[node, String], maxIterations: Int, sc : SparkContext): Graph[node, String] = {

      //var myGraph = randomize_ids(g, sc).cache()
      var myGraph = g

      var counter = 0
      val fields = new TripletFields(true, true, false) //join strategy

      println("ITERATION NUMERO : " + (counter + 1))
      counter += 1

      //We have to create the first knights
      val msg1 = myGraph.aggregateMessages[Long](
        sendtiebreakvalues,
        selectBest,
        fields //use an optimized join strategy (we don't need the edge attribute)
      )

      //This is our main exit condition. We exit if there are no more messages
      if (msg1.isEmpty()) return myGraph

      //Make our vertices into knight candidates
      myGraph = myGraph.joinVertices(msg1)(
        (vid, sommet, bestTb) => markKnight(vid, sommet, bestTb))


      //The marked knights get to choose their best possible color
      //They choose from the lowest colors from adjacent knights
      val unavailableColors = myGraph.aggregateMessages[SortedSet[Int]](
        sendKnightColors,
        mergeColors,
        fields
      )

      myGraph = myGraph.mapVertices( (id, node) => {
        if (node.knighthood == 1)
          new node(id,1, knighthood = 2, tiebreakvalue = node.tiebreakvalue)
        else node
      })


      println("Initial iteration")
      myGraph.vertices.collect().sortBy(_._1).foreach(println)


      def loop1: Unit = {
        while (true) {


          myGraph.vertices.cache()
          myGraph.edges.cache()
          myGraph.cache()

          println("ITERATION NUMERO : " + (counter + 1))
          counter += 1
          if (counter == maxIterations) return

          //We exchange tiebreakers between vertices. Each vertex keeps the biggest tiebreaker.
          val messages = myGraph.aggregateMessages[Long](
            sendtiebreakvalues,
            selectBest,
            fields //use an optimized join strategy (we don't need the edge attribute)
          )

          println("Best tiebreakers for vertices")
          messages.collect.sortBy( _._1) .foreach (println)

          //This is our main exit condition. We exit if there are no more messages
          if (messages.isEmpty()) return

          //Make our vertices into knight candidates
          myGraph = myGraph.joinVertices(messages)(
            (vid, sommet, bestTb) => markKnight(vid, sommet, bestTb))


         println("Knight candidates")
          myGraph.vertices.collect().sortBy(_._1).filter( e=> {
            if (e._2.knighthood == 1) true
            else false
          })foreach(println)

          //The marked knights get to choose their best possible color
          //They choose from the lowest colors from adjacent knights
          val unavailableColors = myGraph.aggregateMessages[SortedSet[Int]](
            sendKnightColors,
            mergeColors,
            fields
          )


         println("Printing unavailable colors")
          unavailableColors.collect().foreach(println)

          //We give their colors to the new knights
            myGraph = myGraph.joinVertices(unavailableColors)(
              (vid, sommet, colors) => becomeKnight(vid, sommet, colors)
            )

          println("Printing graph again")
         myGraph.vertices.collect().sortBy(_._1).foreach(println)

          myGraph.vertices.take(1)
         // myGraph.checkpoint()


        }
      }

      loop1 //execute loop

      //When the graph no longer sends messages, we are almost done.
      //We still have to optimize the last vertices who aren't knights yet.

      val lastKnights = myGraph.aggregateMessages[SortedSet[Int]](sendFinalColors, mergeColors, fields)

      myGraph = myGraph.joinVertices(lastKnights)(
        (vid, sommet, colors) => becomeKnight(vid, sommet, colors))


      println("Printing graph one last time")
      myGraph.vertices.collect().sortBy(_._1).foreach(println)

      myGraph //return the result graph
    }
  }

  object testPetersenGraph extends App {
    val conf = new SparkConf()
      .setAppName("Petersen Graph (10 nodes)")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc.setCheckpointDir("./")
    var myVertices = sc.makeRDD(Array(
      //      (1L, new node(id = 1, tiebreakvalue = 3)), //A
      //      (2L, new node(id = 2, tiebreakvalue = 5)), //B
      //      (3L, new node(id = 3, tiebreakvalue = 1)), //C
      //      (4L, new node(id = 4, tiebreakvalue = 7)), //D
      //      (5L, new node(id = 5, tiebreakvalue = 10)), //E
      //      (6L, new node(id = 6, tiebreakvalue = 2)), //F
      //      (7L, new node(id = 7, tiebreakvalue = 3)), //G
      //      (8L, new node(id = 8, tiebreakvalue = 4)), //H
      //      (9L, new node(id = 9, tiebreakvalue = 6)), //I
      //      (10L, new node(id = 10, tiebreakvalue = 8)))) //J

      (1L, new node(id = 1, tiebreakvalue = 1)), //A
      (2L, new node(id = 2, tiebreakvalue = 2)), //B
      (3L, new node(id = 3, tiebreakvalue = 3)), //C
      (4L, new node(id = 4, tiebreakvalue = 4)), //D
      (5L, new node(id = 5, tiebreakvalue = 5)), //E
      (6L, new node(id = 6, tiebreakvalue = 6)), //F
      (7L, new node(id = 7, tiebreakvalue = 7)), //G
      (8L, new node(id = 8, tiebreakvalue = 8)), //H
      (9L, new node(id = 9, tiebreakvalue = 9)), //I
      (10L, new node(id = 10, tiebreakvalue = 10)))) //J

    var myEdges = sc.makeRDD(Array(
      Edge(1L, 2L, "1"), Edge(1L, 3L, "2"), Edge(1L, 6L, "3"),
      Edge(2L, 7L, "4"), Edge(2L, 8L, "5"),
      Edge(3L, 4L, "6"), Edge(3L, 9L, "7"),
      Edge(4L, 5L, "8"), Edge(4L, 8L, "9"),
      Edge(5L, 6L, "10"), Edge(5L, 7L, "11"),
      Edge(6L, 10L, "12"),
      Edge(7L, 9L, "13"),
      Edge(8L, 10L, "14"),
      Edge(9L, 10L, "15")
    ))

    var myGraph = Graph(myVertices, myEdges)
    val algoColoring = new AlgoColoring()
    val res = algoColoring.execute(myGraph, 2000, sc)
    println("\nNombre de couleur trouvées: " + algoColoring.getChromaticNumber(res))
  }



object testProblem2 extends App {

  val conf = new SparkConf()
    .setAppName("test a problem")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  sc.setCheckpointDir("./")

  var ee = sc.makeRDD(Array(
    Edge(2,1),
    Edge(3,1),
    Edge(3,2),
    Edge(4,1),
    Edge(4,2),
    Edge(4,3),
    Edge(5,2),
    Edge(5,4),
    Edge(6,1),
    Edge(6,3),
    Edge(6,5),
    Edge(7,2),
    Edge(7,4),
    Edge(7,5),
    Edge(7,6),
    Edge(8,1),
    Edge(8,3),
    Edge(8,5),
    Edge(8,6),
    Edge(8,7),
    Edge(9,3),
    Edge(9,4),
    Edge(9,7),
    Edge(9,8),
    Edge(10,1),
    Edge(10,2),
    Edge(10,7),
    Edge(10,8),
    Edge(10,9),
    Edge(11,3),
    Edge(11,4),
    Edge(11,5),
    Edge(11,6),
    Edge(11,9),
    Edge(11,10),
    Edge(12,1),
    Edge(12,2),
    Edge(12,5),
    Edge(12,6),
    Edge(12,9),
    Edge(12,10),
    Edge(12,11)
  ))

  myEdges = ee.map( elem => {
    Edge( elem.srcId, elem.dstId, "")
  })

  var myVertices: RDD[(VertexId, node)] = sc.makeRDD(Array(

    (1L, new node(tiebreakvalue = 5)), //A
    (2L, new node(tiebreakvalue = 4)), //B
    (3L, new node(tiebreakvalue = 7)), //C
    (4L, new node(tiebreakvalue = 12)), //D
    (5L, new node(tiebreakvalue = 11)), //E
    (6L, new node(tiebreakvalue = 8)), //F
    (7L, new node(tiebreakvalue = 1)), //G
    (8L, new node(tiebreakvalue = 2)), //H
    (9L, new node(tiebreakvalue = 9)), //I
    (10L, new node(tiebreakvalue = 6)), //J
    (11L, new node(tiebreakvalue = 3)), //I
    (12L, new node(tiebreakvalue = 10)))) //I


  var myGraph = Graph(myVertices, myEdges)
  val algoColoring = new AlgoColoring()
  val res = algoColoring.execute(myGraph, 2000, sc)
  println("\nNombre de couleur trouvées: " + algoColoring.getChromaticNumber(res))

}