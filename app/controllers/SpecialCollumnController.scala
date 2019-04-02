package controllers

import javax.inject._
import play.api.i18n.I18nSupport

import javax.inject._
import play.api._
import play.api.mvc._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.RuntimeConfig
import com.mongodb.spark._
import java.io._

@Singleton
class SpecialCollumnController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {
  private val db: String = "DOAN"
  private val collection: String = "book"
  private val url = db + "." + collection
  private val sparkSession = SparkSession.builder()
    .master("local")
    .appName("MongoSpark")
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/" + url)
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/" + url)
    .getOrCreate()
  private val df = MongoSpark.load(sparkSession) // Uses the SparkSession

  def index = Action { implicit request =>
    /* For Self-Contained Scala Apps: Create the SparkSession
* CREATED AUTOMATICALLY IN spark-shell */
    var list_key: List[String] = List()
    var count_field = df.count()
    var array_columns = df.columns
    for (i <- 0 to array_columns.length - 1) {
      var field_num = df.select(array_columns(i)).distinct().count()
      if (field_num == count_field) {
        list_key = array_columns(i) :: list_key
        if (array_columns(i) != "_id") {
          var array_field = cut_name(array_columns(i)).split(",")
          if (array_field.length > 1) {
            if (array_field(0).matches(array_columns(i) + ":array")
              || array_field(0).matches(array_columns(i) + ":struct")) {
              array_field = cut_name(array_columns(i)).replaceAll("struct,", "").split(",")
              for (k <- 0 to array_field.length - 1) {
                var temp = array_field(k).substring(0, array_field(k).indexOf(":"))
                array_field.update(k, temp)
              }
              for (k <- 1 to array_field.length - 1) {
                array_field.update(k, array_field(0) + "." + array_field(k))
              }
              for (k <- 1 to array_field.length - 1) {
                var feild_count_2 = df.select(array_field(k)).distinct().count()
                if (feild_count_2 == df.select(array_field(k)).count()) {
                  list_key = array_field(k) :: list_key
                }
              }
            }
          }
        }
      }
    }
    var list_key_new: List[String] = List()
    /*
     * Remove element
     * hoatdong.id_hd
     * hoatdong
     *
     * => hoatdong.id_hd
     */
    for (i <- 0 to list_key.length - 1) {
      for (j <- 0 to list_key.length - 1) {
        if ((i != j) && (list_key(j).indexOf(list_key(i)) == 0)) {
          list_key_new = dropIndex(list_key, i)
        }
      }
    }
    if (list_key_new.length == 0) {
      list_key_new = list_key
    }
    list_key_new.foreach(println)

    println(list_key_new(0), "aaaaaaa")
    Ok(views.html.special_column())
  }

  //    * cut_name:
  //    * Get and cut the excess parts of the string
  //    * struct<books:array<struct<_id:int>>>
  //    * => book:array,_id:int
  def cut_name(name: String): String = {
    var name_list = df.select(name).schema
      .catalogString.mkString.replaceAll("<", ",")
      .replaceAll(">", "")
      .replaceFirst("struct,", "")
    return name_list
  }

  /**
    * Drops the 'i'th element of a list.
    */
  def dropIndex[T](xs: List[T], n: Int) = {
    val (l1, l2) = xs splitAt n
    l1 ::: (l2 drop 1)
  }
}

