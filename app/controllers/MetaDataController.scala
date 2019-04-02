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

//case class Relationship(fieldName: String, `type`: String)
//
//case class Length(min: Int, max: Int)
//
//case class Field(var name: String, `type`: String, nullable: String, length: Length, relationship: Relationship)
//
//case class Table(var name: String, primaryKey: String, fields: Seq[Field], relationship: Relationship)

@Singleton
class MetaDataController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {

  // Form
  import play.api.data.Forms._
  import play.api.data.Form
  import WidgetForm._

  private var tables = List[Table]();
  private var primaryKeyTabels = List[Map[String, String]]();
  private var path = "";

  private var hostname: String = _;
  private var post: String = _;
  private var dbname: String = _;


  //  check connection to mongodb
  def checkConnectionMGDB = Action(parse.tolerantFormUrlEncoded) { request =>
    var listCollectionNames: List[String] = List[String]()
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

    //   index
//    def index() = Action { implicit request =>
//
//      val hostname = request.body.get("hostname").map(_.head).get
//      val port = request.body.get("port").map(_.head).get
//      val dbname = request.body.get("dbname").map(_.head).get
//
//      var mongoClient: MongoClient = MongoClient("mongodb://" + hostname + ":" + port)
//
//      val database: MongoDatabase = mongoClient.getDatabase(dbname)
//
//      tables = List[Table]();
//
//      database.getCollection("book").createIndex(Indexes.ascending("title"), IndexOptions().unique(true));
//
//      database.getCollection("book").find().subscribe(new Observer[Document] {
//        override def onNext(doc: Document) = {
//          println(doc.get("title").get.asString.getValue)
//        }
//
//        override def onError(e: Throwable) = println(s"Error getDataFromDistinctField!${e}")
//
//        override def onComplete() = {}
//
//      })
//
//      //    database.getCollection("book").listIndexes.subscribe(new Observer[Any] {
//      //            override def onNext(doc: Any) = {
//      //              println(doc)
//      //            }
//      //
//      //            override def onError(e: Throwable) = println(s"Error getDataFromDistinctField!${e}")
//      //
//      //            override def onComplete() = {}
//      //
//      //          })
//
//      implicit var lengthWrites: Writes[Length] = (
//        (JsPath \ "min").write[Int] and
//          (JsPath \ "max").write[Int]
//        ) (unlift(Length.unapply))
//
//      implicit var relationshipWrites: Writes[Relationship] = (
//        (JsPath \ "fieldName").write[String] and
//          (JsPath \ "type").write[String]
//        ) (unlift(Relationship.unapply))
//
//      implicit var fieldWrites: Writes[Field] = (
//        (JsPath \ "name").write[String] and
//          (JsPath \ "type").write[String] and
//          (JsPath \ "nullable").write[String] and
//          (JsPath \ "length").write[Length] and
//          (JsPath \ "relationship").write[Relationship]
//        ) (unlift(Field.unapply))
//
//      implicit var tableWrites: Writes[Table] = (
//        (JsPath \ "name").write[String] and
//          (JsPath \ "primaryKey").write[String] and
//          (JsPath \ "fields").write[Seq[Field]] and
//          (JsPath \ "relationship").write[Relationship]
//        ) (unlift(Table.unapply))
//
//      val filePath = "C:/Users/WINDOWS/Desktop/PlayFW/doan3/app/controllers/metadata.json"
//
//      val jsonString = Source.fromFile(filePath).mkString;
//
//      val json: List[Map[String, Any]] = scala.util.parsing.json.JSON.parseFull(jsonString).get.asInstanceOf[List[Map[String, Any]]]
//
//      for (table <- json) {
//        path = table.get("name").get.toString
//        println(path)
//        val fields = table.get("fields").get.asInstanceOf[List[Field]]
//        getListFields(fields, table.get("name").get.toString)
//      }
//
//      //    tables.map(
//      //      table => {
//      //        if (table.name == "table_1") {
//      //          table.fields.map(field => {
//      //            if(field.name == "MSSV") {
//      //              println(field);
//      //              field.name = "MSSV1"
//      //              field
//      //            }
//      //          })
//      //          table
//      //        }
//      //      }
//      //    )
//      //                val writer = new PrintWriter(new File(filePath))
//      //                writer.write(Json.toJson(tables).toString)
//      //                writer.close()
//      Ok(views.html.metadata(tables))
//    }
  //
  //  def getData(primaryKey: String, pathEmbededDoc: String): List[String] = {
  //    var arrayPath: Array[String] = pathEmbededDoc.split(",").map(_.trim)
  //
  //    var data: List[String] = List[String]();
  //    //    if (arrayPath.size.toString == "3") {
  //    val results = database.getCollection(arrayPath(0)).find().limit(10);
  //    results.subscribe(new Observer[Document] {
  //      override def onNext(doc: Document) = {
  //        data = data :+ doc.get(arrayPath(1)).get.asDocument().get("name").asString.getValue
  //      }
  //
  //      override def onError(e: Throwable) = println(s"Error getDataFromDistinctField!${e}")
  //
  //      override def onComplete() = {}
  //
  //    })
  //    Await.result(results.toFuture, Duration.Inf)
  //    //    }
  //    data
  //  }
  //
  //  def getLengthOfField(fieldName: String, pathEmbededDoc: String): Length = {
  //    var arrayPath: Array[String] = pathEmbededDoc.split(",").map(_.trim)
  //    var data: List[String] = List[String]();
  //
  //    val results = database.getCollection(arrayPath(0)).find();
  //    //    data = data :+ doc.get(fieldName).get.asString.getValue
  //    results.subscribe(new Observer[Document] {
  //      override def onNext(doc: Document) = {
  //        var dataTemp: String = "";
  //
  //        // bad code
  //        if (arrayPath.size == 1) {
  //          val docDataAnyType = doc.get(fieldName).get
  //
  //
  //          if (docDataAnyType.isString()) {
  //
  //            data = data :+ docDataAnyType.asString.getValue
  //          } else if (docDataAnyType.isDouble()) {
  //            data = data :+ docDataAnyType.asDouble.getValue.toString
  //          }
  //
  //        } else if (arrayPath.size == 2) {
  //
  //          val docDataAnyType = doc.get(arrayPath(1)).get.asDocument().get(fieldName);
  //
  //          if (docDataAnyType.isString()) {
  //            data = data :+ docDataAnyType.asString.getValue
  //          } else if (docDataAnyType.isDouble()) {
  //            data = data :+ docDataAnyType.asDouble.getValue.toString
  //          }
  //        }
  //      }
  //
  //      override def onError(e: Throwable) = println(s"Error ${pathEmbededDoc} ${fieldName} !${e}")
  //
  //      override def onComplete() = {}
  //
  //    })
  //    Await.ready(results.toFuture, Duration.Inf)
  //    var min: Int = 0;
  //    var max: Int = 0;
  //    //    println(data, path, fieldName, arrayPath.size)
  //    if (!data.isEmpty) {
  //      max = data.map(_.length).max
  //      min = data.map(_.length).min
  //    }
  //    Length(min, max)
  //  }
  //
  //  def getListFields(fields: List[Any], tableName: String, findRelationship: Boolean = false, pathEmbededDoc: String = path): Unit = {
  //    var listFields = List[Field]();
  //    // lấy field đầu tiên làm primarykey
  //    var primaryKey = getPrimaryKey(fields)
  //
  //    // relationship
  //    var relativeField: String = "";
  //    var relativeType: String = "";
  //    if (findRelationship) {
  //      var data = getData(primaryKey, pathEmbededDoc);
  //
  //      var listDuplicates: List[String] = data.diff(data.distinct).distinct;
  //
  //      if (listDuplicates.size == 0) {
  //        relativeField = tableName;
  //        relativeType = "1_1"
  //      } else {
  //        relativeField = tableName;
  //        relativeType = "n_1"
  //      }
  //    }
  //    fields.foreach(field => {
  //      var fieldMap: Map[String, Any] = field.asInstanceOf[Map[String, Any]];
  //
  //      // type of field
  //      var typeFiled: String = "";
  //
  //      var length: Length = Length(0, 0);
  //      // kiểm tra type của field
  //      fieldMap.get("type").get match {
  //        case type1: Map[String, Any] => {
  //
  //          // nếu type of field là "struct"
  //          if (type1.get("type").get == "struct") {
  //            //
  //
  //            relativeField = fieldMap.get("name").get.toString;
  //            relativeType = "1_1";
  //
  //            //
  //            typeFiled = "struct";
  //
  //            val sub_fields = type1.get("fields").get.asInstanceOf[List[Map[String, Any]]]
  //
  //            var pathEmbededDocAr = pathEmbededDoc + "," + fieldMap.get("name").get.toString;
  //
  //            getListFields(sub_fields, fieldMap.get("name").get.toString, true, pathEmbededDocAr)
  //
  //            // nếu type of field là "array"
  //          } else if (type1.get("type").get == "array") {
  //            //
  //            relativeField = fieldMap.get("name").get.toString;
  //            relativeType = "1_n";
  //
  //            //
  //            typeFiled = "array";
  //
  //            val elementType = type1.get("elementType").get.asInstanceOf[Map[String, Any]]
  //
  //            val sub_fields = elementType.get("fields").get.asInstanceOf[List[Map[String, Any]]]
  //
  //            var pathEmbededDocAr = pathEmbededDoc + "," + fieldMap.get("name");
  //
  //            getListFields(sub_fields, fieldMap.get("name").get.toString, true, pathEmbededDocAr)
  //
  //          }
  //        }
  //        case _ => {
  //          typeFiled = fieldMap.get("type").get.toString
  //
  //          if (fieldMap.get("name").get.toString != "_id") {
  //            length = getLengthOfField(fieldMap.get("name").get.toString, pathEmbededDoc)
  //          }
  //
  //
  //        }
  //      }
  //
  //      // lấy thông tin field đưa vào map
  //      listFields = listFields :+ Field(
  //        fieldMap.get("name").get.toString,
  //        typeFiled.toString,
  //        fieldMap.get("nullable").get.toString,
  //        length,
  //        Relationship("", "")
  //      );
  //
  //    })
  //    // set table name
  //
  //    addTable(listFields, tableName, primaryKey, relativeField, relativeType);
  //    addPrimaryKey(primaryKey, tableName)
  //
  //
  //  }
  //
  //  def addTable(listFields: Seq[Field], tableName: String, primaryKey: String, relativeField: String, relativeType: String): Unit = {
  //    // tạo table
  //    var table: Table = Table(tableName, primaryKey, listFields, Relationship(relativeField, relativeType))
  //    tables = table :: tables
  //  }
  //
  //  def addPrimaryKey(primaryKey: String, tableName: String): Unit = {
  //
  //    primaryKeyTabels = Map(
  //      "table_name" -> tableName,
  //      "primaryKey" -> primaryKey,
  //    ) :: primaryKeyTabels
  //  }
  //
  //  def getPrimaryKey(fields: List[Any]): String = {
  //
  //    var primaryKey: String = "";
  //
  //    fields.foreach(field => {
  //      var fieldMap: Map[String, Any] = field.asInstanceOf[Map[String, Any]];
  //
  //      // type of field
  //      var typeFiled: String = "";
  //
  //      // kiểm tra type của field
  //      if (fieldMap.get("type").get != "struct" &&
  //        fieldMap.get("type").get != "array" &&
  //        fieldMap.get("name").get != "_id") {
  //        primaryKey = fieldMap.get("name").get.toString;
  //
  //        return primaryKey;
  //      }
  //
  //
  //    });
  //    primaryKey
  //
  //  }
}


