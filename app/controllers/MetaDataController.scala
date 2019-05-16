package controllers

import java.util

import play.api._
import play.api.mvc._
import javax.inject._
import play.api.i18n.I18nSupport

//import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkConf
//import org.apache.spark.SparkContext
//
//import scala.util.control.Breaks._
//import scala.util.parsing.json._
//import scala.io.Source
//import org.apache.spark._
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.StreamingContext._
//import com.mongodb.spark.MongoSpark
import org.mongodb.scala.bson.BsonDocument
import org.mongodb.scala.{Completed, Observer, documentToUntypedDocument}
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala.model.Filters._
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import org.mongodb.scala.bson.BsonValue
import play.api.data._
import play.api.data.Forms._


import play.api.libs.functional.syntax._

import org.mongodb.scala.model.Aggregates._

import org.mongodb.scala.model._
import play.api.libs.json._

import play.api.libs.json.Reads._ // Custom validation helpers
import play.api.libs.functional.syntax._ // Combinator syntax

import scala.io.Source
import java.io.File
import java.io.PrintWriter

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DatabaseMetaData;
@Singleton
class MetaDataController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {

  // Form
  import play.api.data.Forms._
  import play.api.data.Form
  import WidgetForm._

  //  check connection to mongodb
  def checkConnectionMGDB = Action(parse.tolerantFormUrlEncoded) { request =>
    var listCollectionNames: List[String] = List[String]()
    var tables = List[Table]();
    var connectionStatus = "failure";
    try {
      val hostname = request.body.get("hostname").map(_.head).get
      val port = request.body.get("port").map(_.head).get
      val dbname = request.body.get("dbname").map(_.head).get

      var mongoClient: MongoClient = MongoClient("mongodb://" + hostname + ":" + port)

      val database: MongoDatabase = mongoClient.getDatabase(dbname)



      // get name of collections
      val results = database.listCollectionNames()

      results.subscribe(new Observer[String] {
        override def onNext(collectionName: String) = {
          listCollectionNames = listCollectionNames :+ collectionName
        }

        override def onError(e: Throwable) = Ok(Json.obj("status" -> "500", "message" -> ("Not found.")))

        override def onComplete() = {}

      })
      Await.result(results.toFuture, Duration(1000, "millis"))
    } catch {
      case e: Exception => Ok(Json.obj("status" -> "KO", "message" -> ("Place '' saved.")))
    }

    var message: String = "Successful connection";
    var status: Int = 200;
    //     nếu trống
    if (listCollectionNames.isEmpty) {
      message = "Fail connection"
      status = 500;
    }
    Ok(Json.obj("status" -> status, "message" -> (message), "collectionNames" -> Json.toJson(listCollectionNames)))
  }

//   check mysql
  def checkConnectionMYSQL() = Action(parse.tolerantFormUrlEncoded) { request =>
    var tables: Array[String] = Array[String]();
//    try {
      var hostname = request.body.get("hostname").map(_.head).get;

      var port = request.body.get("port").map(_.head).get;
      var dbname = request.body.get("dbname").map(_.head).get;
      val url: String = "jdbc:mysql://" + hostname + ":" + port + "/" + dbname;
      val driver = "com.mysql.jdbc.Driver";
      val username = request.body.get("username").map(_.head).get;
      val password = request.body.get("password").map(_.head).get;
      Class.forName(driver);

      val connection = DriverManager.getConnection(url, username, password);

      var statement = connection.createStatement;

//
//    var rsa = statement.executeQuery("SELECT * FROM answer");
//    var rsMetaData = rsa.getMetaData();
//
//    val numberOfColumns: Int = rsMetaData.getColumnCount();
//    var i = 0;
//    for (  i <- 1 until numberOfColumns) {
//      println(rsMetaData.getCatalogName(i));
//      println(rsMetaData.getSchemaName(i));
//  }



    var metaData: DatabaseMetaData  = connection.getMetaData();
    var foreignKeys: ResultSet  = metaData.getImportedKeys(connection.getCatalog(), null, "anwser");
   while (foreignKeys.next()) {
      println(foreignKeys.getString("PKTABLE_CAT"));
     println("---------------------------")
     println(foreignKeys.getString("PKTABLE_SCHEM"));
     println("---------------------------")
     println(foreignKeys.getString("PKTABLE_NAME"));
     println("---------------------------")
     println(foreignKeys.getString("PKCOLUMN_NAME"));
     println("---------------------------")
     println(foreignKeys.getString("FKTABLE_CAT"));
     println("---------------------------")
     println(foreignKeys.getString("FKTABLE_SCHEM"));
     println("---------------------------")
     println(foreignKeys.getString("FKTABLE_NAME"));
     println("---------------------------")
     println(foreignKeys.getString("FKCOLUMN_NAME"));
     println("---------------------------")
     println(foreignKeys.getString("KEY_SEQ"));
     println("---------------------------")

     println(foreignKeys.getString("FK_NAME"));
     println("---------------------------")

     println(foreignKeys.getString("PK_NAME"));
     println("---------------------------")

    }
//
      var rs = statement.executeQuery("SHOW TABLES");

      while (rs.next) {

        val tableName = rs.getString(1);
        tables = tables :+ tableName;
      }
//    } catch {
//      case e: Exception => Ok(Json.obj("status" -> "KO", "message" -> ("Place '' saved.")))
//    }
    var message: String = "Successful connection";
    var status: Int = 200;
    //     nếu trống
    if (tables.isEmpty) {
      message = "Fail connection"
      status = 500;
    }
    Ok(Json.obj("status" -> status, "message" -> (message), "tableNames" -> Json.toJson(tables)))
  }
}


