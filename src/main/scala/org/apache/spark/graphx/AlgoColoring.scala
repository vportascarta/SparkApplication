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
  import org.apache.spark.graphx.{Edge, EdgeContext, Graph, _}

  import scala.collection.mutable.ArrayBuffer

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
        return new node(sommet.id, sommet.color + 1, 0, sommet.tiebreakvalue)
      }
    }

    //We go through all the edges.
    //When an edge connects a knight to a knight candidate, we send the color
    def sendKnightColors(ctx : EdgeContext[node, String, ArrayBuffer[Int]]) : Unit =
    {
      val srcColor = ctx.srcAttr.color
      val dstColor = ctx.dstAttr.color

      //SRC is knight candidate and DST is knight
      if (ctx.srcAttr.knighthood == 1 && ctx.dstAttr.knighthood == 2) {

        val t = new ArrayBuffer[Int]()
        t += dstColor
        ctx.sendToSrc( t)
      }

      //DST is knight candidate and SRC is knight
      if (ctx.dstAttr.knighthood == 1 && ctx.srcAttr.knighthood == 2) {
        val v = new ArrayBuffer[Int]()
        v += srcColor
        ctx.sendToDst(  v)
      }

    }

    //Send final colors for the last knights
    def sendFinalColors(ctx : EdgeContext[node, String, ArrayBuffer[Int]]) : Unit =
    {
      val srcColor = ctx.srcAttr.color
      val dstColor = ctx.dstAttr.color

      if (ctx.srcAttr.knighthood == 0) {
        val t = new ArrayBuffer[Int]()
        t += dstColor
        ctx.sendToSrc(t)
      }

      if (ctx.dstAttr.knighthood == 0) {
        val v = new ArrayBuffer[Int]()
        v += srcColor
        ctx.sendToDst(v)
      }
    }


    //We merge two colors vectors together. We just want to have our colors in order, so that selecting the lowest possible color is very fast.
    //https://en.wikipedia.org/wiki/Merge_sort (parallel)
    //O(n)
    def mergeColors(a: ArrayBuffer[Int], b: ArrayBuffer[Int]): ArrayBuffer[Int] = {
      val new_vector = new ArrayBuffer[Int]()
      var i = 0 //index de la map a
      var j = 0 //index de la map b

      def func(): Unit = {
        while (true) {
          if (i == a.length || a.isEmpty) return
          if (j == b.length || b.isEmpty) return

          //Comparer a et b
          val color_a = a(i)
          val color_b = b(j)

          //A = [1,2,4]  B=[2,3,4]  (Juste un vector de couleurs)
          //Cas 1 : Le vecteur A, a la position courante, a une plus petite couleur. On l'ajoute direct
          if (color_a < color_b) {
            //Ajouter la couleur dans le new vector
            new_vector.append(color_a)
            i += 1
          }

          //Cas 2 : Même couleur. On fait avancer les deux vecteurs
          else if (color_a == color_b) {
            new_vector.append(color_a)
            j += 1
            i += 1
          }

          //Case 3 : B a la plus petite couleur.
          else {
            new_vector.append(color_b)
            j += 1
          }
        }
      } //fin func

      //Start the loop process
      func()

      //Deverser le restant de A dans newvector
      while (i != a.length) {
        new_vector.append(a(i))
        i += 1
      }

      //Deverser le restant de B dans newvector
      while (j != b.length) {
        new_vector.append(b(j))
        j += 1
      }
      //Return le resultat
      new_vector
    }

    //Become a knight
    def becomeKnight(myid: Long, myNode: node, colors: ArrayBuffer[Int]): node =
    {
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

      var newNode = new node(id = myNode.id, knighthood = 2, color = newColor, tiebreakvalue = myNode.tiebreakvalue)
      newNode
    }


    def execute(g: Graph[node, String], maxIterations: Int, sc : SparkContext): Graph[node, String] = {

      var myGraph = randomize_ids(g, sc).cache()
      var counter = 0
      val fields = new TripletFields(true, true, false) //join strategy

      def loop1: Unit = {
        while (true) {

          //println("ITERATION NUMERO : " + (counter + 1))
          counter += 1
          if (counter == maxIterations) return

          //We exchange tiebreakers between vertices. Each vertex keeps the biggest tiebreaker.
          val messages = myGraph.aggregateMessages[Long](
            sendtiebreakvalues,
            selectBest,
            fields //use an optimized join strategy (we don't need the edge attribute)
          )

          //println("Best tiebreakers for vertices")
          //messages.collect.sortBy( _._1) .foreach (println)

          //This is our main exit condition. We exit if there are no more messages
          if (messages.isEmpty()) return

          //Make our vertices into knight candidates
          myGraph = myGraph.joinVertices(messages)(
            (vid, sommet, bestTb) => markKnight(vid, sommet, bestTb))


          //myGraph.vertices.collect().sortBy(_._1).foreach(println)

          //The marked knights get to choose their best possible color
          //They choose from the lowest colors from adjacent knights
          val unavailableColors = myGraph.aggregateMessages[ArrayBuffer[Int]](
            sendKnightColors,
            mergeColors,
            fields
          )

          //println("Printing unavailable colors")
          //unavailableColors.collect().sortBy(_._1).foreach(println)

          //If this is the first iteration, we have to proceed differently. There are no true knights yet.
          if (counter == 1) {
            myGraph = myGraph.mapVertices( (id, node) => {
              if (node.knighthood == 1)
                new node(id, node.color, knighthood = 2, tiebreakvalue = node.tiebreakvalue)
              else node
            })
          }

          //We give their colors to the new knights
          else
          {
            myGraph = myGraph.joinVertices(unavailableColors)(
              (vid, sommet, colors) => becomeKnight(vid, sommet, colors)
            )
          }


          //println("Printing graph again")
         // myGraph.vertices.collect().sortBy(_._1).foreach(println)


          //Ignorez : Code de debug
          //var printedGraph = myGraph.vertices.collect()
          // printedGraph = printedGraph.sortBy(_._1)
          //  printedGraph.foreach(
          //    elem => println(elem._2)
          //   )
        }
      }

      loop1 //execute loop

      //When the graph no longer sends messages, we are almost done.
      //We still have to optimize the last vertices who aren't knights yet.

      val lastKnights = myGraph.aggregateMessages[ArrayBuffer[Int]](sendFinalColors, mergeColors, fields)

      myGraph = myGraph.joinVertices(lastKnights)(
        (vid, sommet, colors) => becomeKnight(vid, sommet, colors))


     // println("Printing graph one last time")
     // myGraph.vertices.collect().sortBy(_._1).foreach(println)

      myGraph //return the result graph
    }
  }

  object testPetersenGraph extends App {
    val conf = new SparkConf()
      .setAppName("Petersen Graph (10 nodes)")
      .setMaster("local[1]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
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