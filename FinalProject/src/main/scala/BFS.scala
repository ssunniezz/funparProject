import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters.*
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet}

object BFS {
  type CMap[K, V] = collection.concurrent.Map[K,V]
  type Map[K, V] = collection.Map[K,V]
  type Set[V] = collection.Set[V]

  def avg_timed[A](f: => A): (Double, A) = {
    val start = System.nanoTime
    (1 until 10).foreach(_ => f)
    val res = f
    val stop = System.nanoTime
    (((stop - start)/1e9)/10, res)
  }
  case class Node(
                   key: Char,
                   row: Int,
                   col: Int,
                 )
    extends  Comparable[Node] {
    override def compareTo(o: Node): Int = this.row.compareTo(o.row)
  }
  def solveMaze(maze: Vector[String], vBfs: (Node => Set[Node], Node) => (Map[Node,Node], Map[Node, Int])): Option[String] = {
    def getNbrs(maze: Vector[String], v: Node): Set[Node] = {
      if v.key.equals('x') then Set()
      else
        val Node(k, r, c) = v
        Set( (r-1, c),(r, c-1),(r, c+1),(r+1, c) )
          .filter((r,c) => (r>=0 && r<maze.length) && (c>=0 && c<maze(0).length))
          .map((r, c) => Node(maze(r).charAt(c), r, c))
          .filter(n => !n.key.equals('x'))
    }
    def findDir(src: Node, target: Node): Char = {
      val (Node(ks,rs,cs), Node(kt,rt,ct)) = (src, target)
      if rs < rt then 'u'
      else if rs > rt then 'd'
      else if cs < ct then 'l'
      else 'r'
    }
    def walkBack(parent: Map[Node, Node], cur: Node, next: Node, ans: String): String = {
      if next.key.equals('s') then ans + findDir(cur, next)
      else walkBack(parent, next, parent(next), ans + findDir(cur, next))
    }

    maze match {
      case Vector() => None
      case _ =>
        val rows = maze.map(_.contains('s')).indexOf(true)
        val cols = maze(rows).indexOf('s')
        val s = Node(maze(rows).charAt(cols), rows, cols)
        val (parent, distance) = vBfs(v => getNbrs(maze, v), s)
        val rowe = maze.map(_.contains('e')).indexOf(true)
        val cole = maze(rowe).indexOf('e')
        val e = Node(maze(rowe).charAt(cole), rowe, cole)
        if !distance.contains(e) then
          None
        else
          Some(walkBack(parent, e, parent(e), "").reverse)
    }

  }

  def bfs[V](nbrs: V => Set[V], src: V): (Map[V,V], Map[V, Int]) = {

    def expand(frontier: Set[V], parent: Map[V, V]): (Set[V], Map[V, V]) = {
      def addToParent(vertex: V, adj: Set[V], parent: Map[V, V]): Map[V, V] = adj.toList match {
        case Nil => parent
        case h::t =>
          if parent.contains(h) then addToParent(vertex, t.toSet, parent)
          else addToParent(vertex, t.toSet, parent + (h -> vertex))
      }

      (frontier.foldLeft(Set.empty)((s, x) => s ++ nbrs(x).filter(n => !parent.contains(n))),  frontier.foldLeft(parent)((m,x )=> addToParent(x, nbrs(x), m)))
    }

    def iterate(frontier: Set[V],
                parent: Map[V, V],
                distance: Map[V, Int], d: Int,
                ): (Map[V, V], Map[V, Int]) =
      if frontier.isEmpty then
        (parent, distance)
      else {
        val (frontier_, parent_) = expand(frontier, parent)
        val distance_ = frontier.foldLeft(distance)(
        (m,x) => if distance.contains(x) then m
                  else m + (x -> d))
        iterate(frontier_, parent_, distance_, d+1)
    }

    iterate(Set(src), Map(src -> src), Map(), 0)
  }

  def bfsFut[V](nbrs: V => Set[V], src: V): (CMap[V,V], CMap[V, Int]) = {

    def expandFut(frontier: Set[V], parent: CMap[V, V], fixed: Set[V]): (Set[V], CMap[V, V]) = {
      val allFrontier = ConcurrentSkipListSet[V]()

      val nextSet = frontier.map(
        v => Future{
          nbrs(v).foreach(f => {
            if !parent.contains(f) then parent += (f -> v)
            if !fixed.contains(f) then allFrontier.add(f)
          })})

      val nextSetFut = Future.sequence(nextSet)
      val _ = Await.result(nextSetFut, Duration.Inf)

      (allFrontier.asScala.toSet, parent)
    }

    def iterate(frontier: Set[V],
      parent: CMap[V, V],
      distance: CMap[V, Int], d: Int): (CMap[V, V], CMap[V, Int]) =
      if frontier.isEmpty then
        (parent, distance)
      else {

        Future { frontier.foreach(x =>
        if distance.contains(x) then distance
        else distance += (x -> d) )}

        val (frontier_, parent_) = expandFut(frontier, parent, parent.keySet.toSet)
        iterate(frontier_, parent_, distance, d+1)
      }

    val parent = new ConcurrentHashMap[V,V]().asScala
    val distance = new ConcurrentHashMap[V, Int]().asScala
    val (par, dist) = iterate(Set(src), parent += (src -> src), distance, 0)
    (par, dist)
  }

  def bfsThr[V](nbrs: V => Set[V], src: V): (Map[V,V], Map[V, Int]) = {

    def execute[F](body: => F)(implicit ec: ExecutionContext): Future[F] = {
      val p = Promise[F]

      try {
        ec.execute( new Runnable {
          override def run(): Unit = p.success(body)
        })
      } catch {
        case ex => p.failure(ex)
      }

      p.future
    }

    class Counter {
      private var count = 0

      def increment() = this.synchronized( this.count += 1 )
      def get = this.synchronized( count )
    }

    def expandThr(frontier: Set[V], parent: CMap[V, V], fixed: Set[V]): (Set[V], CMap[V, V]) = {55555
      val frontFinish = new Counter

      val allFrontier = ConcurrentSkipListSet[V]()

      val _ = frontier.map(
        v => execute {
          nbrs(v).foreach(f =>
            if !parent.contains(f) then parent += (f -> v)
            if !fixed.contains(f) then allFrontier.add(f))

          frontFinish.increment()
//          frontFinish.synchronized( frontFinish.notify() )
        }
      )

      while (frontFinish.get < frontier.size) {
//        frontFinish.synchronized( frontFinish.wait() )
      }

      (allFrontier.asScala.toSet, parent)
    }

    def iterate(frontier: Set[V],
                parent: CMap[V, V],
                distance: CMap[V, Int], d: Int,
               ): (CMap[V, V], CMap[V, Int]) =
      if frontier.isEmpty then
        (parent, distance)
      else {

        execute { frontier.foreach(x =>
        if distance.contains(x) then distance
        else distance += (x -> d) )}

        val (frontier_, parent_) = expandThr(frontier, parent, parent.keySet.toSet)
        iterate(frontier_, parent_, distance, d+1)
      }

    val parent = new ConcurrentHashMap[V,V]().asScala
    val distance = new ConcurrentHashMap[V, Int]().asScala
    val (par, dist) = iterate(Set(src), parent += (src -> src), distance, 0)
    (par, dist)
  }

  val defaultEncoding = "ISO8859-1"

  def load(filename: String): Map[String, Set[String]] = {
    def iterate(l: List[String], count: Int, key: String, value: Set[String], map: Map[String, Set[String]]): Map[String, Set[String]] = l match {
      case Nil => map
      case h::t =>
        val split = h.split('|').map(_.trim)
        if count == 0 then
          iterate(t, split.tail.head.toInt, split.head, Set(), map + (key -> value))
        else
          iterate(t, count-1, key, value ++ split.tail.toSet, map)
    }
    val lines = scala.io.Source.fromFile(filename, defaultEncoding)
      .getLines().drop(1).toList

    val split = lines.head.split('|').map(_.trim)
    iterate(lines.tail, split.tail.head.toInt, split.head, Set(), Map())
  }

  def linkage(thesaurusFile: String, f: (String => Set[String], String) => (Map[String, String], Map[String, Int])): String => String => Option[List[String]] = {
    def walkBack(parent: Map[String, String], src: String, cur: String, ans: List[String]): List[String] = {
      if src.equals(cur) then cur::ans
      else
        walkBack(parent, src, parent(cur), cur::ans)
    }

    (wordB: String) => (wordA: String) =>
      val db = load(thesaurusFile)
      val (parent, distance) = f((v: String) => if db.contains(v) then db(v) else Set(), wordA)
      if !distance.contains(wordB) then None
      else Some(walkBack(parent, wordA, wordB, Nil).reverse)
  }

  def main(args: Array[String]) = {
    val maze = Vector(
      "xxxxxxxxxxxxxxxxxx",
      "xxxxxxxx   exxxxxx",
      "xxxxxx   xxxxxxxxx",
      "xxxxx  xxxxxxxxxxx",
      "xxs   xxxxxxxxxxxx",
      "xxxxxxxxxxxxxxxxxx"
    )

    val maze2 = Vector(
      "xxxxxxxxxxxxxxxxxx",
      "x    x      x   ex",
      "x    x   x  x xxxx",
      "x        x  x    x",
      "xs   x   x       x",
      "xxxxxxxxxxxxxxxxxx"
    )

    val maze3 = Vector(
      "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
      "x    x      x          x      x          x      x    xx    x      x   ex",
      "x    x   x  x xxxxx    x   x  x xxxxx    x   x  x xxxxx    x   x  x xxxx",
      "x        x  x              x  x    xx        x  x    xx        x  x    x",
      "xs   x   x       xx    x   x       xx    x   x             x   x       x",
      "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    )

    /*
    * For some unknown reason, running all measurement continuously is quite unstable (The tests will take longer or faster than expected).
      For fairness, thus, the result of this project will based on measuring each test one by one.
    */

    // Test 1.1
        val (tSeq1, rSeq1) = avg_timed(solveMaze(maze3, bfs))
        println(f"seq: result=$rSeq1, time=$tSeq1%.6fs")

    // Test 1.2
        val (tFut1, rFut1) = avg_timed(solveMaze(maze3, bfsFut))
        println(f"fut: result=$rFut1, time=$tFut1%.6fs")

    // Test 1.3
        val (tThr1, rThr1) = avg_timed(solveMaze(maze3, bfsThr))
        println(f"Thr: result=$rThr1, time=$tThr1%.6fs")

    // Test 2.1
        val (t1, r1) = avg_timed(linkage("thesaurus_db.txt", bfs)("illogical")("logical"))
        println(f"Seq: result=$r1, time=$t1%.6fs")

    // Test 2.2
        val (t2, r2) = avg_timed(linkage("thesaurus_db.txt", bfsFut)("illogical")("logical"))
        println(f"Fut: result=$r2, time=$t2%.6fs")

    // Test 2.3
        val (t3, r3) = avg_timed(linkage("thesaurus_db.txt", bfsThr)("illogical")("logical"))
        println(f"Thr: result=$r3, time=$t3%.6fs")

    /*
    * Below are optional tests.
    */

    //    val (tSeq, rSeq) = timed(solveMaze(maze2, bfs))
    //    println(f"seq: result=$rSeq, time=$tSeq%.6fs")
    //    val (tFut, rFut) = timed(solveMaze(maze2, bfsFut))
    //    println(f"fut: result=$rFut, time=$tFut%.6fs")
    //    val (tThr, rThr) = timed(solveMaze(maze2, bfsThr))
    //    println(f"Thr: result=$rThr, time=$tThr%.6fs")


    //    val (t1, r1) = avg_timed(linkage("thesaurus_db.txt", bfs)("life")("happiness"))
    //    println(f"Seq: result=$r1, time=$t1%.6fs")
    //    val (t2, r2) = avg_timed(linkage("thesaurus_db.txt", bfsFut)("life")("happiness"))
    //    println(f"Fut: result=$r2, time=$t2%.6fs")
    //    val (t3, r3) = avg_timed(linkage("thesaurus_db.txt", bfsThr)("life")("happiness"))
    //    println(f"Thr: result=$r3, time=$t3%.6fs")
  }
}
