import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.lib.PageRank
import java.io._

object PageRankSpark {
  def main(args: Array[String]) {
    val startTime = System.nanoTime
    if (args.length < 1) {
      //graph file name, number of iterations to run PageRank
      System.err.println("Usage: PageRank <file> <iter>")
      System.exit(1)
    }

    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()	
    val sc = spark.sparkContext

    // Load the edges as a graph - note that Spark assumes a directed edge list
    val graph = GraphLoader.edgeListFile(sc, args(0))
    // Run PageRank
    val loadTime = System.nanoTime
    val output_graph = PageRank.run(graph, args(1).toInt)
    val numVertex = output_graph.numVertices.toFloat

    // Sort the vertices by PageRank and normalize, then write to a string for output
    val result = output_graph.vertices.map(item => item.swap).sortByKey(false, 1).map(item => item.swap).map(item => (item._1, item._2 / numVertex)).map{case(int, float) => int.toString + "\t" + float.toString}.collect().take(25).mkString("\n")

    val loadDuration = (System.nanoTime - startTime) / 1e9d
    val computeDuration = (System.nanoTime - loadTime) / 1e9d
    val computeDurationPerIteration = computeDuration / args(1).toInt.toFloat
    println("Load Time: " + loadDuration.toString + " Compute Time: " + computeDuration.toString + " Compute Time Per Iteration: " + computeDurationPerIteration)
    //print the results to screen and output them to a file
    println(result)
    val writer = new BufferedWriter(new FileWriter(new File("spark_output_pagerank.txt")))
    writer.write(result)
    writer.close()
    
    spark.stop()
  }
}
