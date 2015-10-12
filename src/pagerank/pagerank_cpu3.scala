import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._

import org.apache.spark.mllib.linalg.{Vector, Vectors, Matrices,Matrix}
import org.apache.spark.mllib.linalg.distributed.RowMatrix

// Usage prog inputfile size outputfile
object PageRank extends App {
    val inputfile = args(0).toString()
    val mat_size = args(1).toInt
    val iter_times = args(2).toInt
    val dpFactor = 0.85

    val spConfig = (new SparkConf).setMaster("local").setAppName("SparkPR")
    val sc = new SparkContext(spConfig)
    val rows = sc.textFile(inputfile).map { line =>
        line.split(' ').map(_.toDouble)
        }
    val rows2 = rows.map(row => row.map( x => x*dpFactor + (1-dpFactor)/mat_size))

    val rows3 = rows2.map{row => Vectors.dense(row)}
    val rmat = new RowMatrix(rows3)
    rmat.rows.foreach(println)
    var pArray = Array.fill[Double](mat_size)(1.0)
    for (i <- 1 to iter_times){
        val pVect:Matrix = Matrices.dense(mat_size,1,pArray)
        val projected: RowMatrix = rmat.multiply(pVect)
        //projected.rows.foreach(println)
        val ret = projected.rows.flatMap(x => x.toArray)
        pArray = ret.collect()
        pArray.foreach(println)
    }
   
}
