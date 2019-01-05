package controllers

import javax.inject._
import play.api.i18n.I18nSupport

import javax.inject._
import play.api._
import play.api.mvc._

import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import java.io._

@Singleton
class SpecialCollumnController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {
  def index = Action { implicit request =>
    val db : String = "test"
    val collection : String = "type"
    val url = db+"."+collection
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSpark")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/"+ url)
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/"+ url)
      .getOrCreate()
    val df = MongoSpark.load(sparkSession)  // Uses the SparkSession

    var count_field = df.count()
    val array_collums = df.columns
    var array_key = new Array[String](array_collums.length)
    var i = 0
    var j,k = 0
    var ac =  df.columns
    for ( i <- 0 to array_collums.length-1){
      var feild_num = df.select(array_collums(i)).distinct().count()
      if (feild_num == count_field){
        array_key.update(j,array_collums(i))
        j += 1
        if(array_collums(i) != "_id") {
          ac = df.select(array_collums(i)).schema
            .catalogString.mkString.replaceAll("<", ",")
            .replaceAll(">", "")
            .replaceFirst("struct,", "")
            .split(",")
          if (ac(0).matches(array_collums(i) + ":array")) {
            ac = df.select(array_collums(i)).schema
              .catalogString.mkString.replaceAll("<", ",")
              .replaceAll(">", "")
              .replaceAll("struct,", "")
              .split(",")
          }else if(ac(0).matches(array_collums(i) + ":struct")){
            ac = df.select(array_collums(i)).schema
              .catalogString.mkString.replaceAll("<", ",")
              .replaceAll(">", "")
              .replaceFirst("struct,", "")
              .split(",")  //get collums and type
          }
          for (k <- 0 to ac.length - 1) {
            ac(k) = ac(k).substring(0, ac(k).indexOf(":")) //get name collums
          }
          for (k <- 1 to ac.length - 1) {
            ac(k) = ac(0) + "." + ac(k) // name collection.collums (sinhvien.mssv)
          }
          for (k <- 1 to ac.length - 1) {
            var feild_count = df.select(ac(k)).distinct().count()
            if (feild_count == count_field) {
              array_key.update(j, ac(k))
              j += 1
            }
          }
        }
      }
    }
    var n = 0
    array_key.foreach(println)

    for ( i <- 0 to array_key.length-1){
      var max_length = 0;
      if(array_key(i) != null){
        var gt = df.select(array_key(i)).collect() // get values
        var min_length = gt(0).mkString.length
        for ( i <- 0 to gt.length-1){
          if(gt(i).mkString.length > max_length) {
            max_length = gt(i).mkString.length
          }
          if(gt(i).mkString.length < min_length) {
            min_length = gt(i).mkString.length
          }
        }
        println(min_length,max_length)
      }
    }
    Ok(views.html.special_column())
  }
}

