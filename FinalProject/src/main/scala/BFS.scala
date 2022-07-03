import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.collection.JavaConverters._

import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListSet}

object BFS {
  type CMap[K, V] = collection.concurrent.Map[K,V]

  def timed[A](f: => A): (Double, A) = {
    val start = System.nanoTime
    val res = f
    val stop = System.nanoTime
    ((stop - start)/1e9, res)
  }
  case class Node(
                   key: Char,
                   row: Int,
                   col: Int,
                 )
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

  def bfsFut[V](nbrs: V => Set[V], src: V): (Map[V,V], Map[V, Int]) = {

    def expandFut(frontier: Set[V], parent: CMap[V, V], fixed: Map[V,V]): (Set[V], CMap[V, V]) = {
      Future{frontier.foldLeft(parent)((m,x) =>
          nbrs(x).foldLeft(m)((mm, adj) =>
            if mm.contains(adj) then mm
            else mm += (adj -> x)
          ))}

      val nextSet = frontier.map(
        v => Future{ nbrs(v).filter(n => !fixed.contains(n)) }
      )
      val nextSetFut = Future.sequence(nextSet)

      val allFrontier = nextSetFut.map(
        s => s.foldLeft(Set.empty: Set[V])((ss, elem) => ss ++ elem)
      )

      (Await.result(allFrontier, Duration.Inf), parent)
    }

    def iterate(frontier: Set[V],
    parent: CMap[V, V],
    distance: CMap[V, Int], d: Int,
                ): (CMap[V, V], CMap[V, Int]) =
      if frontier.isEmpty then
        (parent, distance)
      else {

        val temp = Future{expandFut(frontier, parent, parent.toMap)}
        frontier.map(x => Future{
          if distance.contains(x) then distance
          else distance += (x -> d)
        })

        val (frontier_, parent_) = Await.result(temp, Duration.Inf)
        iterate(frontier_, parent_, distance, d+1)
      }

    val parent = new ConcurrentHashMap[V,V]().asScala
    val distance = new ConcurrentHashMap[V, Int]().asScala
    val (par, dist) = iterate(Set(src), parent += (src -> src), distance, 0)
    (par.toMap, dist.toMap)
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

    val (tSeq1, rSeq1) = timed(solveMaze(maze2, bfs))
    val (tFut1, rFut1) = timed(solveMaze(maze2, bfsFut))

    println(f"seq: result=$rSeq1, time=$tSeq1%.6fs")
    println(f"fut: result=$rFut1, time=$tFut1%.6fs")

    val (tSeq, rSeq) = timed(solveMaze(maze, bfs))
    val (tFut, rFut) = timed(solveMaze(maze, bfsFut))


    println(f"seq: result=$rSeq, time=$tSeq%.6fs")
    println(f"fut: result=$rFut, time=$tFut%.6fs")
  }
}
