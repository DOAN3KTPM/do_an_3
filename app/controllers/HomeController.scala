package controllers

import play.api.libs.json.JsValue
import play.api.libs.json._

import javax.inject._
import play.api.i18n.I18nSupport

import javax.inject._
import play.api._
import scala.collection.JavaConversions._

import play.api.mvc._
import java.sql.DriverManager
import java.sql.Connection
import scala.runtime.ScalaRunTime._
import play.api.data._
import play.api.data.Forms._
import play.api.data.validation.Constraints._
import play.api.mvc.Session
import scala.concurrent.Await
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.duration.Duration // should remove
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId, BsonString}
import org.mongodb.scala.{Completed, Document, MongoClient, MongoCollection, Observer, MongoDatabase}

//import Conn._

import play.api.libs.json.Reads._ // Custom validation helpers
import play.api.libs.functional.syntax._ // Combinator syntax

// printer
import scala.io.Source
import java.io.File
import java.io.PrintWriter

// Apache spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.RuntimeConfig
import com.mongodb.spark._
import java.io._

import scala.util.parsing.json.JSON.parseFull


// đạt
import com.mongodb.casbah.Imports._
import scala.io.Source
import net.liftweb.json._
import function.my_function._
import java.sql.{DriverManager, ResultSet}
import java.text.SimpleDateFormat
import java.util.Calendar
case class Relationship(fieldName: String, `type`: String)

case class RelationshipField(var tableNane: String, var fieldName: String)

case class Length(min: Int, max: Int)

case class Field(var name: String, `type`: String, nullable: String, length: Length, var relationship: RelationshipField)

case class Table(var name: String, var primaryKey: String, fields: Seq[Field], var path: String)

@Singleton
class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {

  // Form
  import play.api.data.Forms._
  import play.api.data.Form
  import WidgetForm._

  // can remove
  //  private var url: String = _;
  //  private var driver: String = "com.mysql.jdbc.Driver";
  //  private var username: String = "root";
  //  private var password: String = "";
  private var dbname: String = "";
  //  private var tables: Array[String] = Array[String]();


  private var tables = List[Table]();
  private var primaryKeyTabels = List[Map[String, String]]();

  implicit var lengthWrites: Writes[Length] = (
    (JsPath \ "min").write[Int] and
      (JsPath \ "max").write[Int]
    ) (unlift(Length.unapply))

  implicit var relationshipWrites: Writes[Relationship] = (
    (JsPath \ "fieldName").write[String] and
      (JsPath \ "type").write[String]
    ) (unlift(Relationship.unapply))

  implicit var relationshipFieldWrites: Writes[RelationshipField] = (
    (JsPath \ "tableName").write[String] and
      (JsPath \ "fieldName").write[String]
    ) (unlift(RelationshipField.unapply))

  implicit var fieldWrites: Writes[Field] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "type").write[String] and
      (JsPath \ "nullable").write[String] and
      (JsPath \ "length").write[Length] and
      (JsPath \ "relationship").write[RelationshipField]
    ) (unlift(Field.unapply))

  implicit var tableWrites: Writes[Table] = (
    (JsPath \ "name").write[String] and
      (JsPath \ "primaryKey").write[String] and
      (JsPath \ "fields").write[Seq[Field]] and
        (JsPath \ "path").write[String]
    //      (JsPath \ "relationship").write[Relationship]
    ) (unlift(Table.unapply))

  def index() = Action { implicit request =>
    Ok(views.html.index())
  }


  def getMetadataOfCollection() = Action(parse.tolerantFormUrlEncoded) { implicit request =>
    // khởi tạo lại rỗng
    tables = List[Table]()

    val hostname = request.body.get("hostname").map(_.head).get
    val port = request.body.get("port").map(_.head).get
    dbname = request.body.get("dbname").map(_.head).get
    //  List collections ( List[String] )
    var listCollectionNames: List[String] = parseFull(request.body.get("collectionNames")
      .map(_.head).get).get.asInstanceOf[List[String]]

    var mongoClient: MongoClient = MongoClient("mongodb://" + hostname + ":" + port)

    val database: MongoDatabase = mongoClient.getDatabase(dbname)

    for (collectionName <- listCollectionNames) {

      // init apache spark
      var sparkSession = SparkSession.builder()
        .master("local")
        .appName("getMetadataOfCollection" + collectionName)
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/" + dbname + "." + collectionName)
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/" + dbname + "." + collectionName)
        .getOrCreate()

      // find relationship between tables
      //    findRelationsBetweenSpecificTablesToOthers(book)
      var df = MongoSpark.load(sparkSession) // Uses the SparkSession


      // metadata
      var jsonSchemaAddName = df.schema.prettyJson.replaceFirst(""""type"""", """"name" : """" + collectionName +""""""" + ",\n" +""""type"""")
      // chuyển json  to array json
      jsonSchemaAddName = "[" + jsonSchemaAddName.toString + "]"
      // parsing string to json
      val json: List[Map[String, Any]] = parseFull(jsonSchemaAddName).get.asInstanceOf[List[Map[String, Any]]]

      // loop collections from json
      for (table <- json) {
        // name of collection
        val path = table.get("name").get.toString
        val fields = table.get("fields").get.asInstanceOf[List[Field]]

        getListFields(fields, table.get("name").get.toString, database, path)
      }
      sparkSession.close()
    }

    var searched_collections = listCollectionNames;
    for (collection_name <- listCollectionNames) {

      // get unique columns
      var unique_columns: List[String] = getUniqueColumnsOfTable(dbname, collection_name)

      var results = database.getCollection(collection_name).find.limit(10);
      results.subscribe(new Observer[Document] {
        override def onNext(doc: Document) = {
          // looping others to find relationship
          for (searched_collection_name <- searched_collections) {

            if (searched_collection_name != collection_name) {
              var columnsTypeArray = getColumnsTypeArray(searched_collection_name)
              var columnsTypeNormal = getColumnsTypeNormal(searched_collection_name) // INT, STRING ...

              // loop column (cr)
              for (column_name <- unique_columns) {


                if (column_name != "_id") {
                  var doc_value = doc.get(column_name).get.asString.getValue

                  // find in collumns
                  for (collumnName <- columnsTypeArray) {

                    // get values of collumn
                    var isNext = true
                    var searchValue = database.getCollection(searched_collection_name).find();
                    searchValue.subscribe(new Observer[Document] {
                      override def onNext(doc_search: Document) = {

                        // lấy các giá trị của doccument có type là array ([1, 2, 3, 4] => List(1,2,3,4))
                        var listValues = List[String]()

                        // lấy value của 1 column
                        var valuesOfDocColumnTypeArray = doc_search.get(collumnName).get.asArray().getValues();
                        valuesOfDocColumnTypeArray.toList.foreach(value => {

                          // kiểm tra value có phải là String hay k
                          if (value.isInstanceOf[BsonString]) {
                            listValues = listValues :+ value.asString.getValue.toString
                            // nếu value là objectId
                          } else if (value.isInstanceOf[BsonObjectId]) {
                            listValues = listValues :+ value.asObjectId.getValue.toString
                          }

                        })
                        // Found

                        if (listValues.contains(doc_value)) {
                          setPrimaryKeyForTable(collection_name, column_name)
                          setFoKeyForeignKeyForTable(searched_collection_name, collumnName, collection_name, column_name)
                        }


                      }

                      override def onError(e: Throwable) = println(s"Error searchValue not id!${e}")

                      override def onComplete() = {}

                    }) // end get values of collumn
                    Await.ready(searchValue.toFuture, Duration.Inf)


                  } // end find in collumns

                  // find in comlumn 1-1
                  for (collumnName <- columnsTypeNormal) {
                    var searchValue = database.getCollection(searched_collection_name).find();
                    searchValue.subscribe(new Observer[Document] {
                      override def onNext(doc_search: Document) = {
                        // lấy các giá trị của doccument có type là array ([1, 2, 3, 4] => List(1,2,3,4))
                        val value = doc_search.get(collumnName).get;

                        var value_search = ""
                        if (value.isInstanceOf[BsonString]) {
                          value_search = doc_search.get(collumnName).get.asString().getValue
                          // nếu value là objectId
                        } else if (value.isInstanceOf[BsonObjectId]) {
                          value_search = doc_search.get(collumnName).get.asObjectId.getValue.toString
                        }


                        // Found
                        if (value_search == doc_value) {
                          setPrimaryKeyForTable(collection_name, column_name)
                          setFoKeyForeignKeyForTable(searched_collection_name, collumnName, collection_name, column_name)
                        }


                      }

                      override def onError(e: Throwable) = println(s"Error searchValue 1!${e}")

                      override def onComplete() = {}

                    }) // end get values of collumn
                    Await.ready(searchValue.toFuture, Duration.Inf)

                  }
                } else {
                  // checking "_id" column must BsonObjectId type
                  if (doc.get(column_name).get.isInstanceOf[BsonObjectId]) {
                    var doc_id = doc.get("_id").get.asObjectId.getValue.toString;


                    // find in collumns 1_n
                    for (collumnName <- columnsTypeArray) {

                      // get values of collumn

                      var searchValue = database.getCollection(searched_collection_name).find();
                      searchValue.subscribe(new Observer[Document] {
                        override def onNext(doc_search: Document) = {


                          // lấy các giá trị của doccument có type là array ([1, 2, 3, 4] => List(1,2,3,4))
                          var listValues = List[String]()

                          // lấy value của 1 column
                          var valuesOfDocColumnTypeArray = doc_search.get(collumnName).get.asArray().getValues();
                          valuesOfDocColumnTypeArray.toList.foreach(value => {

                            // kiểm tra value có phải là String hay k
                            if (value.isInstanceOf[BsonString]) {
                              listValues = listValues :+ value.asString.getValue.toString
                              // nếu value là objectId
                            } else if (value.isInstanceOf[BsonObjectId]) {
                              listValues = listValues :+ value.asObjectId.getValue.toString
                            }

                          })

                          // Found
                          if (listValues.contains(doc_id)) {
                            setPrimaryKeyForTable(collection_name, column_name)
                            setFoKeyForeignKeyForTable(searched_collection_name, collumnName, collection_name, column_name)
                          }


                        }

                        override def onError(e: Throwable) = println(s"Error searchValue 2!${e}")

                        override def onComplete() = {}

                      }) // end get values of collumn
                      Await.ready(searchValue.toFuture, Duration.Inf)


                    } // end find in collumns

                    // find in comlumn 1-1
                    for (collumnName <- columnsTypeNormal) {
                      var searchValue = database.getCollection(searched_collection_name).find();
                      searchValue.subscribe(new Observer[Document] {
                        override def onNext(doc_search: Document) = {

                          // lấy các giá trị của doccument có type là array ([1, 2, 3, 4] => List(1,2,3,4))
                          val value = doc_search.get(collumnName).get;
                          var value_search = ""
                          if (value.isInstanceOf[BsonString]) {
                            value_search = doc_search.get(collumnName).get.asString().getValue
                            // nếu value là objectId
                          } else if (value.isInstanceOf[BsonObjectId]) {
                            value_search = doc_search.get(collumnName).get.asObjectId.getValue.toString
                          }

                          // Found
                          if (value_search == doc_id) {
                            setPrimaryKeyForTable(collection_name, column_name)
                            setFoKeyForeignKeyForTable(searched_collection_name, collumnName, collection_name, column_name)
                          }


                        }

                        override def onError(e: Throwable) = println(s"Error searchValue 3!${e}")

                        override def onComplete() = {}

                      }) // end get values of collumn
                      Await.ready(searchValue.toFuture, Duration.Inf)

                    }
                  }
                }
              } // end loop column

            }


          } // end looping others to find relationship
        }

        override def onError(e: Throwable) = println(s"Error ${unique_columns} Get unique!${e}")

        override def onComplete() = {
          Ok(Json.obj("status" -> "200", "message" -> "Success", "tables" -> Json.toJson(tables)))
        }

      })
      Await.ready(results.toFuture, Duration.Inf)
    }

    setPrimaryKeyForTables()
    Thread.sleep(500)

    Ok(Json.obj("status" -> "200", "message" -> "Success", "tables" -> Json.toJson(tables)))
  }

  //
  def setPrimaryKeyForTable(table_name: String, column_name: String): Unit = {
    for (table <- tables) {
      // tìm thay table
      if (table.name == table_name) {
        // lặp các field để tìm field có type là array
        table.primaryKey = column_name
      }
    }
  }

  //
  def setPrimaryKeyForTables(): Unit = {
    tables.foreach((table: Table) => {


      var list_foreign_names = List[String]();
      tables.foreach((table: Table) => {

        table.fields.foreach((field: Field) => {
          list_foreign_names = list_foreign_names :+ field.relationship.fieldName;
        })
      })


      if (list_foreign_names.contains(table.primaryKey)) {
        table.primaryKey = "_id"
      }
    })
  }

  //
  def setFoKeyForeignKeyForTable(foreign_table_name: String, foreign_key: String, table_name: String, column_name: String) {
    for (table <- tables) {
      // tìm thay table
      if (table.name == foreign_table_name) {
        // lặp các field để tìm field có type là array
        for (field <- table.fields) {

          if (field.name == foreign_key) {
            field.relationship.tableNane = table_name
            field.relationship.fieldName = column_name
          }
        }

      }
    }
  }

  //
  def getColumnsTypeArray(colectionName: String): List[String] = {
    var columnsTypeArray = List[String]();
    // loop tables

    tables.foreach((table) => {
      // tìm thay table
      if (colectionName == table.name) {
        // lặp các field để tìm field có type là array
        for (field <- table.fields) {
          //          println(field.`type`, field.name)
          // nếu là array
          if (field.`type` == "array") {
            columnsTypeArray = columnsTypeArray :+ field.name
          }
        }
      }
    }) // end loop tables
    columnsTypeArray
  }

  def getColumnsTypeNormal(colectionName: String): List[String] = {
    var columnsTypeArray = List[String]();
    // loop tables

    tables.foreach((table) => {
      // tìm thay table
      if (colectionName == table.name) {
        // lặp các field để tìm field có type là array
        for (field <- table.fields) {

          if (field.`type` != "array" && field.`type` != "") {
            columnsTypeArray = columnsTypeArray :+ field.name
          }
        }
      }
    }) // end loop tables
    columnsTypeArray
  }


  // get fields of specific table
  def getListFields(fields: List[Any], tableName: String, database: MongoDatabase, pathEmbededDoc: String, findRelationship: Boolean = false): Unit = {
    var listFields = List[Field]();
    // lấy field đầu tiên làm primarykey
    //    val list_fields_uniqued = getUniqueColumnsOfTable(dbname, tableName)
    var primaryKey = getPrimaryKey(fields)
    // relationship
    var relativeField: String = "";
    var relativeType: String = "";


    fields.foreach(field => {
      var fieldMap: Map[String, Any] = field.asInstanceOf[Map[String, Any]];
      // type of field
      var typeFiled: String = "";

      var length: Length = Length(0, 0);
      // kiểm tra type của field
      fieldMap.get("type").get match {
        case type1: Map[String, Any] => {

          // nếu type of field là "struct"
          if (type1.get("type").get == "struct" && fieldMap.get("name").get.toString != "_id") {
            //

            relativeField = fieldMap.get("name").get.toString;
            relativeType = "1_1";

            //
            typeFiled = "struct";

            val sub_fields = type1.get("fields").get.asInstanceOf[List[Map[String, Any]]]

            var path_table = pathEmbededDoc + "/" + fieldMap.get("name").get.toString;

            getListFields(sub_fields, fieldMap.get("name").get.toString, database, path_table, true)

            // nếu type of field là "array"
          }
          else if (type1.get("type").get == "array") {
            //
            relativeField = fieldMap.get("name").get.toString;
            relativeType = "1_n";

            //
            typeFiled = "array";

            val elementType = type1.get("elementType").get.asInstanceOf[Map[String, Any]]

            val sub_fields = elementType.get("fields").get.asInstanceOf[List[Map[String, Any]]]


            var path_table: String = pathEmbededDoc + "," + fieldMap.get("name").get.toString;

            getListFields(sub_fields, fieldMap.get("name").get.toString, database, path_table, true)

          } else {
            listFields = listFields :+ Field(
              fieldMap.get("name").get.toString,
              typeFiled.toString,
              fieldMap.get("nullable").get.toString,
              length,
              RelationshipField("", "")
            );
          }
        }
        case _ => {
          typeFiled = fieldMap.get("type").get.toString

          //          if (fieldMap.get("name").get.toString != "_id") {
          //            length = getLengthOfField(fieldMap.get("name").get.toString, pathEmbededDoc, database)
          //          }
          // lấy thông tin field đưa vào map
          listFields = listFields :+ Field(
            fieldMap.get("name").get.toString,
            typeFiled.toString,
            fieldMap.get("nullable").get.toString,
            length,
            RelationshipField("", "")
          );

        }
      }



    })
    // set table name

    addTable(listFields, tableName, primaryKey, relativeField, relativeType, pathEmbededDoc);
    addPrimaryKey(primaryKey, tableName)
  }

  def addTable(listFields: Seq[Field], tableName: String, primaryKey: String, relativeField: String, relativeType: String, path: String): Unit = {
    // tạo table

    //    var table: Table = Table(tableName, primaryKey, listFields, Relationship(relativeField, relativeType))
    var table: Table = Table(tableName, primaryKey, listFields, path)

    // không add các collection có field "oid
    if (listFields.filter(_.name == "oid").isEmpty) {
      tables = table :: tables
    }

  }

  def addPrimaryKey(primaryKey: String, tableName: String): Unit = {
    primaryKeyTabels = Map(
      "table_name" -> tableName,
      "primaryKey" -> primaryKey,
    ) :: primaryKeyTabels
  }

  def getPrimaryKey(fields: List[Any]): String = {
    var primaryKey: String = "_id";

    fields.foreach(field => {
      var fieldMap: Map[String, Any] = field.asInstanceOf[Map[String, Any]];


      // kiểm tra type của field
      if (fieldMap.get("type").get != "struct" &&
        fieldMap.get("type").get != "array" &&
        fieldMap.get("name").get != "_id" &&
        fieldMap.get("type").get.isInstanceOf[String]) {

//        primaryKey = fieldMap.get("name").get.toString;
        primaryKey = "_id";

        return primaryKey;
      }


    });

    primaryKey
  }

  def getData(primaryKey: String, pathEmbededDoc: String, database: MongoDatabase): List[String] = {
    var arrayPath: Array[String] = pathEmbededDoc.split(",").map(_.trim)

    var data: List[String] = List[String]();
    //    if (arrayPath.size.toString == "3") {
    val results = database.getCollection(arrayPath(0)).find().limit(10);
    results.subscribe(new Observer[Document] {
      override def onNext(doc: Document) = {
        data = data :+ doc.get(arrayPath(1)).get.asDocument().get("name").asString.getValue
      }

      override def onError(e: Throwable) = println(s"Error getDataFromDistinctField!${e}")

      override def onComplete() = {}

    })
    Await.ready(results.toFuture, Duration.Inf)
    //    }
    data
  }


  def getLengthOfField(fieldName: String, pathEmbededDoc: String, database: MongoDatabase): Length = {
    var arrayPath: Array[String] = pathEmbededDoc.split(",").map(_.trim)
    var data: List[String] = List[String]();

    val results = database.getCollection(arrayPath(0)).find();
    //    data = data :+ doc.get(fieldName).get.asString.getValue
    results.subscribe(new Observer[Document] {
      override def onNext(doc: Document) = {
        var dataTemp: String = "";

        // bad code
        if (arrayPath.size == 1) {
          val docDataAnyType = doc.get(fieldName).get


          if (docDataAnyType.isString()) {

            data = data :+ docDataAnyType.asString.getValue
          } else if (docDataAnyType.isDouble()) {
            data = data :+ docDataAnyType.asDouble.getValue.toString
          }

        } else if (arrayPath.size == 2) {

          val docDataAnyType = doc.get(arrayPath(1)).get.asDocument().get(fieldName);

          if (docDataAnyType.isString()) {
            data = data :+ docDataAnyType.asString.getValue
          } else if (docDataAnyType.isDouble()) {
            data = data :+ docDataAnyType.asDouble.getValue.toString
          }
        }
      }

      override def onError(e: Throwable) = println(s"Error getLengthOfField ${pathEmbededDoc} ${fieldName} !${e}")

      override def onComplete() = {}

    })
    Await.ready(results.toFuture, Duration.Inf)
    var min: Int = 0;
    var max: Int = 0;
    //    println(data, path, fieldName, arrayPath.size)
    if (!data.isEmpty) {
      max = data.map(_.length).max
      min = data.map(_.length).min
    }
    Length(min, max)
  }

  // get unique columns
  def getUniqueColumns(df: Dataset[Row]): List[String] = {
    println("==========================================")
    println("==========================================")
    var list_key: List[String] = List()
    var count_field = df.count()
    var array_columns = df.columns

    for (i <- 0 to array_columns.length - 1) {
      var field_num = df.select(array_columns(i)).distinct().count()
      println(array_columns(i), "=====")
      //      if (field_num == count_field) {
      //        list_key = array_columns(i) :: list_key
      //        if (array_columns(i) != "_id") {
      //          var array_field = cut_name(array_columns(i), df).split(",")
      //          if (array_field.length > 1) {
      //            if (array_field(0).matches(array_columns(i) + ":array")
      //              || array_field(0).matches(array_columns(i) + ":struct")) {
      //              array_field = cut_name(array_columns(i), df).replaceAll("struct,", "").split(",")
      //              for (k <- 0 to array_field.length - 1) {
      //                var temp = array_field(k).substring(0, array_field(k).indexOf(":"))
      //                array_field.update(k, temp)
      //              }
      //              for (k <- 1 to array_field.length - 1) {
      //                array_field.update(k, array_field(0) + "." + array_field(k))
      //              }
      //              for (k <- 1 to array_field.length - 1) {
      //                var feild_count_2 = df.select(array_field(k)).distinct().count()
      //                if (feild_count_2 == df.select(array_field(k)).count()) {
      //                  list_key = array_field(k) :: list_key
      //                }
      //              }
      //            }
      //          }
      //        }
      //      }
    }
    var list_key_new: List[String] = List()
    /*
     * Remove element
     * hoatdong.id_hd
     * hoatdong
     *
     * => hoatdong.id_hd
     */
    //    for (i <- 0 to list_key.length - 1) {
    //      for (j <- 0 to list_key.length - 1) {
    //        if ((i != j) && (list_key(j).indexOf(list_key(i)) == 0)) {
    //          list_key_new = dropIndex(list_key, i)
    //        }
    //      }
    //    }
    //    if (list_key_new.length == 0) {
    //      list_key_new = list_key
    //    }
    list_key_new
  }


  //  ==================== Unities ===================
  def cut_name(name: String, df: Dataset[Row]): String = {
    var name_list = df.select(name).schema
      .catalogString.mkString.replaceAll("<", ",")
      .replaceAll(">", "")
      .replaceFirst("struct,", "")
    return name_list
  }



  //  đạt ===============================================================================
  var t1 = System.nanoTime()









  def getUniqueColumnsOfTable(db: String, collection: String): List[String] = {

    val re: String = "\"type\""

    var database1 = "\"" + db + ":" + collection + "\""
    val a: String = "\"database\" : " + database1 + ",\n\"name\" : \"" + collection + "\"" + ",\n" + "\"type\""
    val url = db + "." + collection
    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("MongoSpark")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/" + url)
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/" + url)
      .getOrCreate()
    val df = MongoSpark.load(sparkSession) // Uses the SparkSession
    val ac = df.schema
    val ac_tree = ac.treeString.replaceFirst("root", collection)
    var av = ac.json
    //println(av)
    val jsonString = ac.prettyJson
    val json_new = jsonString.replaceFirst(re, a)
    //        println(json_new)
    //        val pw = new PrintWriter(new File("D:\\DoAn3\\Json\\Output_"+collection+".json" ))
    //        pw.write(json_new)
    //        pw.close


    //        val lines = Source.fromFile("D:\\DoAn3\\Json\\Output_book.json").mkString
    var jsonData = net.liftweb.json.parse(json_new)
    var num = countElement("fields", "name", jsonData)

    var list_all_name = getElement("name", jsonData)
    var list_type = getElement("type", jsonData)
    var list_new: List[String] = List()
    var list_name: List[String] = List()
    for (i <- 0 to num - 1) {
      list_new = getName(jsonData, i) + ":" + getType(jsonData, i) :: list_new
    }
    list_new = list_new.filterNot((a: String) => {
      a.contains("array")
    })
    var list_find: List[String] = List()
    var list_key: List[String] = List()
    for (i <- 0 to list_new.length - 1) {
      var temp = list_new(i).substring(0, list_new(i).indexOf(":"))
      if (temp != "_id") {
        list_find = temp :: list_find
      } else {
        list_key = temp :: list_key
      }
    }
    var database2 = getElement("database", jsonData)
    val db1 = database2(0).substring(0, database2(0).indexOf(":"))
    val collection1 = database2(0).substring(database2(0).indexOf(":") + 1, database2(0).length)
    //connect to database
    val client: MongoClient = MongoClient()
    val mongoConn = MongoConnection()
    val mongoColl = mongoConn(db1)(collection1)
    val size = mongoColl.size
    for (i <- 0 to list_find.length - 1) {
      var tam = mongoColl.distinct(list_find(i)).size
      if (tam == size) {
        list_key = list_find(i) :: list_key
      }
    }
    list_key
  }

  def start() = Action(parse.tolerantFormUrlEncoded) { implicit request =>
    var lines = request.body.get("dataArrayMongodb").map(_.head).get
    val json = net.liftweb.json.parse(lines)
    var list_collection = getElement("key", json)
    var database = request.body.get("dbname").map(_.head).get
    var db_name: String = database
    var i = 0
    if (CheckDBExist(db_name)) {
      while (CheckDBExist(db_name) == true) {
        if (CheckDBExist(db_name)) {
          db_name = database + "_" + i
          i += 1
        } else {
          db_name = database
        }
      }
    }else{
      db_name = database
    }
    //    *******Create Database*******
//    println(db_name)
    Create_DB("CREATE DATABASE " + db_name)
    //    *****************************
    val conn_str = "jdbc:mysql://localhost:3306/" + db_name + "?user=root&password="
    // Load the driver
    classOf[com.mysql.jdbc.Driver]
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    var linkdata = lines.substring(lines.indexOf("\"linkDataArray\": [")+20)
    var p_from : List[String] = List()

    if(linkdata != ""){
      linkdata = linkdata.substring(0, linkdata.length - 4)
      linkdata = linkdata.replaceAll("[\\r\\n]", "")
      var array_data = linkdata.split("},")
      for(i <-0 to array_data.length -1 ){
        if (array_data(i) != "" || array_data(i) != " ") {
          if (!array_data(i).contains("}")) {
            array_data(i) += "}"
          }
          var parse_linkdata = net.liftweb.json.parse(array_data(i))
          p_from = getElement("fromPort", parse_linkdata)(0) :: p_from
        }
      }
    }
    println(p_from)
    //    val client: MongoClient = MongoClient()
    var list_js = lines.substring(lines.indexOf(": [")+3,lines.indexOf("\"linkDataArray\"")-6)
    var list_json = list_js.split("[\\r\\n]")
    var key,collect = ""
    for (h <- 0 to list_json.length -1) {
      if(list_json(h) != "" && list_json(h) != " ") {
        val js = net.liftweb.json.parse(list_json(h))
        val collection = getElement("path",js)
        val mongoConn = MongoConnection()
        if(collection(0).contains(",")){
          key = collection(0).substring(collection(0).indexOf(",")+1)
          collect = collection(0).substring(0,collection(0).indexOf(","))
        }else{
          key = collection(0)
          collect = collection(0)
        }
        val mongoColl = mongoConn(database)(collect)
        val size = mongoColl.size
        var list_all: List[String] = List()
        val list_name = getElement("name", js)
        var list_type = getElement("type", js)
        var l_temp: List[Int] = List()
        var num = countElement("fields", "name", js)
        for (i <- 0 to list_type.length - 1) {
          if (list_type(i).mkString == "array" && list_type(i + 1) == "struct") {
            l_temp = i + 1 :: l_temp
          }
        }
        if (l_temp != null) {
          for (i <- 0 to l_temp.length - 1) {
            list_type = dropIndex(list_type, l_temp(i))
          }
        }
        for (i <- 0 to list_name.length - 1) {
          list_all = list_name(i) + ":" + list_type(i) :: list_all
        }
        //    Create tb 1
        var sql_1 = "CREATE TABLE " + key + "("
        var list_tb1, temp, list_tb1_old: List[String] = List()
        for (i <- 0 to num - 1) {
          list_tb1 = getElement(js, "name", i) + ":" + getType(js, i) :: list_tb1
          if (getElement(js, "rename", i) != "0") {
            list_tb1_old = getElement(js, "rename", i) + ":" + getType(js, i) :: list_tb1_old
          } else {
            list_tb1_old = getElement(js, "name", i) + ":" + getType(js, i) :: list_tb1_old
          }
        }

        val list_id = getElement("primarykey",js)
        var id = ""
        if(list_id.length != 0){
          id = list_id(0)
        }
        var l_tb1: List[String] = List()
        if(key == collect) {
          var res = mongoColl.findOne().mkString
          res = res.substring(1, res.length - 1).replaceAll("\"", "")
          val co = res.count(_ == '{')
          val co2 = res.count(_ == '[')
          if (co > 0) {
            for (i <- 0 to co - 1) {
              res = res.substring(0, res.indexOf('{')) + res.substring(res.indexOf('}') + 1)
            }
          }
          if (co2 > 0) {
            for (i <- 0 to co2 - 1) {
              res = res.substring(0, res.indexOf('[')) + res.substring(res.indexOf(']') + 1)
            }
          }
          var tb1 = res.split(",")
          for (i <- 0 to tb1.length - 1) {
            tb1 = tb1.updated(i, tb1(i).substring(0, tb1(i).indexOf(":")))
            tb1 = tb1.updated(i, tb1(i).substring(1, tb1(i).length - 1))
            for (j <- 0 to list_tb1_old.length - 1) {
              if (list_tb1_old(j).contains(tb1(i))) {
                //          tb1 = tb1.updated(i,list_tb1(j))
                l_tb1 = list_tb1(j) :: l_tb1
              }
            }
          }
        }else{
          l_tb1 = list_tb1
        }
        l_tb1 = l_tb1.reverse
        var n_name = ""
        var n_type = ""
        for (i <- 0 to l_tb1.length - 1) {
          n_name = l_tb1(i).mkString.substring(0, l_tb1(i).indexOf(":"))
          n_type = l_tb1(i).mkString.substring(l_tb1(i).indexOf(":") + 1, l_tb1(i).length)
          sql_1 += n_name + " "
          if (n_name != id) {
            if (n_type == "string" || n_type == "struct" || n_type == "array") {
              if(p_from.contains(n_name)){
                sql_1 += "varchar(255),"
              }else {
                sql_1 += "text,"
              }
            } else {
              if (n_type == "") {
                sql_1 += "varchar(255),"
              } else {
                sql_1 += n_type + ","
              }
            }

          } else {
            if (n_type == "") {
              sql_1 += "varchar(255),"
            } else {
              sql_1 += n_type + ","
            }
          }
        }
        if(id != "") {
          sql_1 = sql_1.substring(0, sql_1.length - 1) + ", PRIMARY KEY (" + id + "))"
        }else{
          sql_1 = sql_1.substring(0, sql_1.length - 1)+")"
        }
        //**** create table 1*****
        try {
          statement.executeUpdate(sql_1)
        } catch {
          case (e: Exception) => Ok(Json.obj("status" -> "400", "message" -> "Không thể tạo quan hệ"))
        }

        for (i <- 0 to list_tb1_old.length - 1) {
          list_tb1_old = list_tb1_old.updated(i, list_tb1_old(i).substring(0, list_tb1_old(i).indexOf(":")))
        }
        var sql_tb = "INSERT INTO " + key + " Values"
        val builder = MongoDBObject.newBuilder
        var dem = 0
        val n = size / 1000
        val q = MongoDBObject.empty
        var k = 0
        if(key == collect) {
          for (i <- 0 to list_tb1_old.length - 1) {
            k = i + 1
            builder += list_tb1_old(i) -> k
          }
          i = 0
          val newObj = builder.result
          var a = ""
          for (x <- mongoColl.find(q, newObj)) {
            if (i <= 1000) {
              a = "("
              a += x.toString
              for (k <- 0 to list_tb1_old.length - 1) {
                a = a.replaceFirst("\"" + list_tb1_old(k) + "\" :", "")
              }

              a = a.replaceAll("[{]", "").replaceAll("[}]", "")
              a = a.substring(a.indexOf(":") + 1)
              a = "(" + a
              a += ")"
              sql_tb += a + ","
              i += 1
              dem += 1
              a = ""
            }
            if (i == 1000 || (i == (size - 1000 * n) && (dem == size))) {
              sql_tb = sql_tb.substring(0, sql_tb.length - 1)
              try {
                statement.executeUpdate(sql_tb)

              } catch {
                case (e: Exception) => Ok(Json.obj("status" -> "400", "message" -> "Không thể tạo quan hệ"))
              }
              i = 0
              sql_tb = "INSERT INTO " + key + " Values"
            }
          }
        }else{
          for (i <- 0 to list_tb1_old.length - 1) {
            k = i + 1
            builder += key+"."+list_tb1_old(i) -> k
          }
          i = 0
          val newObj = builder.result
          val test = mongoColl.findOne(q, newObj)
          if (test.mkString.contains(": [{ ")) {
            var a = ""
            var id_num = ""
            list_tb1_old = list_tb1_old.reverse
            for (x <- mongoColl.find(q, newObj)) {
              if (i <= 1000) {
                id_num = x.toString.substring(x.toString.indexOf("oid") + 8, x.toString.indexOf(key)-6)
                a += x.toString
                a = a.substring(a.indexOf(key)-1)
                a = a.replaceAll("\""+key+"\" : ","")
                a = a.substring(2)
                a = "("+a.replaceAll(" [{] \"\\u0024oid\" :", "")
                //                a = a.replaceAll("\""+list_tb1_old(0)+"\"", "(\""+id_num + "\",")
                for (k <- 0 to list_tb1_old.length - 1) {
                  a = a.replaceAll("\"" + list_tb1_old(k) + "\" :", "")
                }
                a = a. replaceAll("[}], [{]", "), (")
                a = a.replaceAll("[{]", "")
                  .replaceAll("[}]", "")
                //                a = "(\"" + id_num + "\"," + a.substring(a.indexOf(":") + 1, a.length)
                a = a.replaceAll(" , [)],[(]", "), (")
                  .replaceAll(":", "")
                  .replaceAll("]", "")
                a = a.replace(")", ")")
                a += ")"
                sql_tb += a + ","
                i += 1
                dem += 1
                a = ""
                id_num = ""
              }
              if (i == 1000 || (i == (size - 1000 * n) && (dem == size))) {
                sql_tb = sql_tb.substring(0, sql_tb.length - 1)
                try {
                  statement.executeUpdate(sql_tb)
                } catch {
                  case (e: Exception) => Ok(Json.obj("status" -> "400", "message" -> "Không thể tạo quan hệ"))
                }
                i = 0
                sql_tb = "INSERT INTO " + key + " Values"
              }
            }
          } else {
            var a = ""
            var tam = ""
            for (x <- mongoColl.find(q, newObj)) {
              if (i <= 1000) {
                a = x.toString
                a = a.substring(a.indexOf(key)-1)
                for (k <- 0 to list_tb1_old.length - 1) {
                  a = a.replaceFirst("\"" + list_tb1_old(k) + "\" :", "")
                }
                a = a.replaceAll("\""+key+"\"","")
                a = a.replaceAll("[{]", "").replaceAll("[}]", "")
                a = a.replaceFirst(":", "")
                a = a.substring(a.indexOf(":") + 1)
                tam = a.substring(0, a.indexOf(","))
                a = "(" + a.substring(a.indexOf(":") + 1)
                a += ")"
                sql_tb += a + ","
                i += 1
                dem += 1
                a = ""
                tam = ""
              }
              if (i == 1000 || (i == (size - 1000 * n) && (dem == size))) {
                sql_tb = sql_tb.substring(0, sql_tb.length - 1)
                try {
                  statement.executeUpdate(sql_tb)
                }catch{
                  case (e: Exception) => Ok(Json.obj("status" -> "400", "message" -> "Không thể tạo quan hệ"))
                }
                i = 0
                sql_tb = "INSERT INTO " + key + " Values"
              }
            }
          }
        }
      }
    }
    // *******lấy quan hệ
    var relation_json = lines.substring(lines.indexOf("\"linkDataArray\": [")+20)
    if(relation_json != "") {
      relation_json = relation_json.substring(0, relation_json.length - 4)
      relation_json = relation_json.replaceAll("[\\r\\n]", "")
      var relation = relation_json.split("},")
      println(relation_json)
      //***********************
      var add_relation = ""
      for (i <- 0 to relation.length - 1) {
        if (relation(i) != "" || relation(i) != " ") {
          if (!relation(i).contains("}")) {
            relation(i) += "}"
          }
          var list_key: List[String] = List()
          val rel = net.liftweb.json.parse(relation(i))
          val table_from = getElement("from", rel)
          val port_from = getElement("fromPort", rel)
          val table_to = getElement("to", rel)
          val port_to = getElement("toPort", rel)
          list_key = port_from(0) :: list_key
          list_key = port_to(0) :: list_key
          add_relation = "ALTER TABLE " + table_from(0) + " ADD FOREIGN KEY (" + port_from(0)+ ") REFERENCES " + table_to(0) + "(" + port_to(0)  + ");"
          println(add_relation)
          try {
            statement.executeUpdate(add_relation)
          } catch {
            case (e: Exception) => Ok(Json.obj("status" -> "400", "message" -> "Không thể tạo quan hệ"))
          }
        }
      }
    }
    conn.close()
    Ok("")
  }

  def mysql() = Action(parse.tolerantFormUrlEncoded) { implicit request =>
    var lines = request.body.get("dataArrayMysql").map(_.head).get
    val js = net.liftweb.json.parse(lines)
    val list_table = getElement("key", js)
    val hostname = request.body.get("hostname").map(_.head).get
    val database = request.body.get("dbname").map(_.head).get
    val user = request.body.get("username").map(_.head).get
    val pass = request.body.get("password").map(_.head).get
    val port = request.body.get("port").map(_.head).get

    val conn_str = "jdbc:mysql://" + hostname + ":" + port + "/" + database + "?user=" + user + "&password=" + pass
    // Load the driver
    classOf[com.mysql.jdbc.Driver]
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)
    val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
    //    val mongo_url = MongoClientURI("mongodb://127.0.0.1:27017")
    //    val mongoClient: MongoClient = MongoClient(mongo_url)
    val mongoClient = com.mongodb.casbah.Imports.MongoClient("localhost", 27017)
    val format = new SimpleDateFormat("d-M-y")
    var day = format.format(Calendar.getInstance().getTime())
    try{
      val mongoDB = database + "_Convert"
      val db = mongoClient(mongoDB)
      val builder = MongoDBObject.newBuilder
      builder += "ok" -> 1
      val newObj = builder.result
      val mongoConn = MongoConnection()
      for (i <- 0 to list_table.length - 1) {
        db.createCollection(list_table(i), newObj) //create collection in mongodb
        val mongoColl = mongoConn(mongoDB)(list_table(i)) // connect to mongodb  with collection
        var sql = "SELECT * FROM " + list_table(i)
        var results = statement.executeQuery(sql)
        while (results.next()) {
          val numColumns = results.getMetaData.getColumnCount
          var json = "{"
          val builder = MongoDBObject.newBuilder
          for (i <- 1 to numColumns) {
            val columns = results.getMetaData.getColumnName(i)
            var content = results.getObject(columns)
            json += "\"" + columns + "\":\"" + content + "\","
            builder += columns -> content
          }
          json = json.substring(0, json.length - 1) + "}"
          val newObj = builder.result
          try {
            mongoColl.insert(newObj)
          } catch {
            case (e: Exception) => Ok(Json.obj("status" -> "400", "message" -> "Lỗi"))
          }
        }
      }
    }catch {
      case (e: Exception) => Ok(Json.obj("status" -> "400", "message" -> "Không thể tạo cơ sở dữ liệu"))
    }
    Ok("")
  }
}
