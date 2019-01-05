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
class SchemaJsonController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {
  def index= Action { implicit request =>

    val re :String = """"type""""
    val db : String = "DOAN"
    val collection : String = "book"
    val a :String = """"name" : """"+collection+"""""""+",\n\t "+""""type""""
    val url = db+"."+collection
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Spark shell")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/"+ url)
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/"+ url)
      .getOrCreate()
    val df = MongoSpark.load(sparkSession)  // Uses the SparkSession
  var ac = df.schema    //get schema
    println(sparkSession)
  val ac_tree = ac.treeString.replaceFirst("root", collection) // replace root = collection name, print tree schema
//    println(ac_tree, "ac_tree")
//    println(a)
    val jsonString = ac.prettyJson // convert schema to json
  val json_new = jsonString.replaceFirst(re,a) // add collection name to json
//    println(json_new)
    val pw = new PrintWriter(new File("C:\\Users\\WINDOWS\\Desktop\\PlayFW\\doan3\\app\\controllers\\JSON\\Output_"+collection+".json" ))
    pw.write(json_new) // print file
    pw.close

    Ok(views.html.schema())
  }
}
