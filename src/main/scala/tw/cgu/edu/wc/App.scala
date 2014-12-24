package tw.cgu.edu.wc

/**
 * Hello world!
 *
 */
import java.util.Random

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.io.Source

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ ALS, Rating, MatrixFactorizationModel }
import org.apache.spark.mllib.recommendation.ALS  
import org.apache.spark.mllib.recommendation.Rating
object App {
  def main(args: Array[String]) {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // set up environment

    val conf = new SparkConf()
      .setAppName("MovieLensALS")
      .set("spark.executor.memory", "8g")
    //  .setJars(Seq(jarFile))
    val sc = new SparkContext("local[*]", "test")

    // load ratings and movie titles
    val data = sc.textFile("./ratings.dat")
    val ratings = data.map(_.split("::") match {
      case Array(user, item, rate, ts) =>
        Rating(user.toInt, item.toInt, rate.toDouble)
    })
    ratings take 2
    val rank = 10 
  
    val model = ALS.train(ratings, 10,30, 0.01) 
val usersProducts = ratings.map { case Rating(user, product, rate) =>  
       (user, product)  
     }  
    
    
    val predictions =   
       model.predict(usersProducts).map { case Rating(user, product, rate) =>   
         ((user, product), rate)  
       }  
    val ratesAndPreds = ratings.map { case Rating(user, product, rate) =>   
  ((user, product), rate)  
}.join(predictions)  
println(ratesAndPreds.collect().take(100).mkString )
println( model.predict(1, 1))


    sc.stop();
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long) = {
    // ...

  }

  /** Elicitate ratings from command-line. */
  def elicitateRatings(movies: Seq[(Int, String)]) = {
    // ...
  }

}