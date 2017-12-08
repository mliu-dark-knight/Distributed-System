import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.lib.ShortestPaths
import java.io._

object ShortestPathSpark {
  def main(args: Array[String]) {
    val startTime = System.nanoTime
    if (args.length < 1) {
      //graph file name, starting node id
      System.err.println("Usage: ShortestPath <file> <id>")
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
    // Run ShortestPaths
    val loadTime = System.nanoTime
    val output_graph = ShortestPaths.run(graph, Seq(args(1).toInt))

    // sort result by vertex id and convert to string
    val result = output_graph.vertices.sortByKey(true, 1).map{case(int, map) => int.toString + "\t" + map(args(1).toInt).toString}.collect().mkString("\n")

    val loadDuration = (System.nanoTime - startTime) / 1e9d
    val computeDuration = (System.nanoTime - loadTime) / 1e9d
    val computeDurationPerIteration = computeDuration / args(1).toInt.toFloat
    //print results to screen and save to file also
    println(result)
    println("Load Time: " + loadDuration.toString + " Compute Time: " + computeDuration.toString + " Compute Time Per Iteration: " + computeDurationPerIteration)
    val writer = new BufferedWriter(new FileWriter(new File("spark_output_shortest_path.txt")))
    writer.write(result)
    writer.close()
    
    spark.stop()
  }
}
