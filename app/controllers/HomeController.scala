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

/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
case class Relationship(fieldName: String, `type`: String)

case class RelationshipField(var tableNane: String, var fieldName: String)

case class Length(min: Int, max: Int)

case class Field(var name: String, `type`: String, nullable: String, length: Length, relationship: RelationshipField)

case class Table(var name: String, var primaryKey: String, fields: Seq[Field])

@Singleton
class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {

  // Form
  import play.api.data.Forms._
  import play.api.data.Form
  import WidgetForm._

  // can remove
  //  private var url: String = _;
  private var driver: String = "com.mysql.jdbc.Driver";
  private var username: String = "root";
  private var password: String = "";
  //  private var tables: Array[String] = Array[String]();


  private var tables = List[Table]();
  private var primaryKeyTabels = List[Map[String, String]]();
  private var path = "";

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
      (JsPath \ "fields").write[Seq[Field]]
    //      (JsPath \ "relationship").write[Relationship]
    ) (unlift(Table.unapply))

  def index() = Action { implicit request =>
    //    val url: String = "jdbc:mysql://localhost:3306/web_man_fashion";
    //    val driver = "com.mysql.jdbc.Driver";
    //    val username = "root";
    //    val password = "";
    //    Class.forName(driver);
    //    val connection = DriverManager.getConnection(url, username, password);
    //
    //    var statement = connection.createStatement;
    //
    //    var rs = statement.executeQuery("SHOW TABLES");
    //    var tables: Array[String] = Array[String]();
    //    while (rs.next) {
    //      val tableName = rs.getString(1);
    //      tables = tables :+ tableName;
    //    }
    //    Ok(views.html.index(tables, form))

    Ok(views.html.index())
  }


  def getMetadataOfCollection() = Action(parse.tolerantFormUrlEncoded) { implicit request =>
    // khởi tạo lại rỗng
    tables = List[Table]()

    val hostname = request.body.get("hostname").map(_.head).get
    val port = request.body.get("port").map(_.head).get
    val dbname = request.body.get("dbname").map(_.head).get
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
        path = table.get("name").get.toString
        val fields = table.get("fields").get.asInstanceOf[List[Field]]

        getListFields(fields, table.get("name").get.toString, database)
      }
      sparkSession.close()
    }

    for (collection_name_search <- listCollectionNames) {
      var unique_columns: List[String] = List("_id")

      var results = database.getCollection(collection_name_search).find.limit(10);
      results.subscribe(new Observer[Document] {
        override def onNext(doc: Document) = {
          // looping others to find relationship
          for (collection_name <- listCollectionNames) {

            if (collection_name != collection_name_search) {
              var columnsTypeArray = getColumnsTypeArray(collection_name)
              var columnsTypeNormal = getColumnsTypeNormal(collection_name) // INT, STRING ...

              // loop column (cr)
              for (column_name <- unique_columns) {


                if (column_name != "_id") {

                  var doc_value = doc.get(column_name).get.asString.getValue

                  // find in collumns
                  for (collumnName <- columnsTypeArray) {

                    // get values of collumn
                    var isNext = true
                    var searchValue = database.getCollection(collection_name).find();
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
                          setPrimaryKeyForTable(collection_name_search, column_name)
                          setFoKeyForeignKeyForTable(collection_name, collumnName, collection_name_search, column_name)
                        }


                      }

                      override def onError(e: Throwable) = println(s"Error searchValue not id!${e}")

                      override def onComplete() = {}

                    }) // end get values of collumn
                    Await.ready(searchValue.toFuture, Duration.Inf)


                  } // end find in collumns

                  // find in comlumn 1-1
                  for (collumnName <- columnsTypeNormal) {
                    var searchValue = database.getCollection(collection_name).find();
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
                          setPrimaryKeyForTable(collection_name_search, column_name)
                          setFoKeyForeignKeyForTable(collection_name, collumnName, collection_name_search, column_name)
                        }


                      }

                      override def onError(e: Throwable) = println(s"Error searchValue!${e}")

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

                      var searchValue = database.getCollection(collection_name).find();
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
                            setPrimaryKeyForTable(collection_name_search, column_name)
                            setFoKeyForeignKeyForTable(collection_name, collumnName, collection_name_search, column_name)
                          }


                        }

                        override def onError(e: Throwable) = println(s"Error searchValue!${e}")

                        override def onComplete() = {}

                      }) // end get values of collumn
                      Await.ready(searchValue.toFuture, Duration.Inf)


                    } // end find in collumns

                    // find in comlumn 1-1
                    for (collumnName <- columnsTypeNormal) {
                      var searchValue = database.getCollection(collection_name).find();
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
                            setPrimaryKeyForTable(collection_name_search, column_name)
                            setFoKeyForeignKeyForTable(collection_name, collumnName, collection_name_search, column_name)
                          }


                        }

                        override def onError(e: Throwable) = println(s"Error searchValue!${e}")

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

        override def onError(e: Throwable) = println(s"Error Get unique!${e}")

        override def onComplete() = {
          Ok(Json.obj("status" -> "200", "message" -> "Success", "tables" -> Json.toJson(tables)))
        }

      })
      Await.ready(results.toFuture, Duration.Inf)
    }
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
  def setFoKeyForeignKeyForTable(foreign_table_name: String, foreign_key: String, table_name: String, column_name: String) {
    for (table <- tables) {
      // tìm thay table
      if (table.name == foreign_table_name) {
        // lặp các field để tìm field có type là array
        for (field <- table.fields) {

          if (field.name == foreign_key) {

            field.relationship.tableNane = table_name
            field.relationship.fieldName = column_name
            println(field)
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
  def getListFields(fields: List[Any], tableName: String, database: MongoDatabase, findRelationship: Boolean = false, pathEmbededDoc: String = path): Unit = {
    var listFields = List[Field]();
    // lấy field đầu tiên làm primarykey
    var primaryKey = getPrimaryKey(fields)
    // relationship
    var relativeField: String = "";
    var relativeType: String = "";
    //    if (findRelationship) {
    //      var data = getData(primaryKey, pathEmbededDoc, database);
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

            var pathEmbededDocAr = pathEmbededDoc + "," + fieldMap.get("name").get.toString;

            getListFields(sub_fields, fieldMap.get("name").get.toString, database, true, pathEmbededDocAr)

            // nếu type of field là "array"
          }
          else if (type1.get("type").get == "array") {
            //
            relativeField = fieldMap.get("name").get.toString;
            relativeType = "1_n";

            //
            typeFiled = "array";

            //            val elementType = type1.get("elementType").get.asInstanceOf[Map[String, Any]]
            //
            //            val sub_fields = elementType.get("fields").get.asInstanceOf[List[Map[String, Any]]]
            //
            //            var pathEmbededDocAr = pathEmbededDoc + "," + fieldMap.get("name");
            //
            //            getListFields(sub_fields, fieldMap.get("name").get.toString, database, true, pathEmbededDocAr)

          }
        }
        case _ => {
          typeFiled = fieldMap.get("type").get.toString

          //          if (fieldMap.get("name").get.toString != "_id") {
          //            length = getLengthOfField(fieldMap.get("name").get.toString, pathEmbededDoc, database)
          //          }


        }
      }

      // lấy thông tin field đưa vào map
      listFields = listFields :+ Field(
        fieldMap.get("name").get.toString,
        typeFiled.toString,
        fieldMap.get("nullable").get.toString,
        length,
        RelationshipField("", "")
      );

    })
    // set table name

    addTable(listFields, tableName, primaryKey, relativeField, relativeType);
    addPrimaryKey(primaryKey, tableName)


  }

  def addTable(listFields: Seq[Field], tableName: String, primaryKey: String, relativeField: String, relativeType: String): Unit = {
    // tạo table

    //    var table: Table = Table(tableName, primaryKey, listFields, Relationship(relativeField, relativeType))
    var table: Table = Table(tableName, primaryKey, listFields)
    tables = table :: tables
  }

  def addPrimaryKey(primaryKey: String, tableName: String): Unit = {

    primaryKeyTabels = Map(
      "table_name" -> tableName,
      "primaryKey" -> primaryKey,
    ) :: primaryKeyTabels
  }

  def getPrimaryKey(fields: List[Any]): String = {

    var primaryKey: String = "";

    fields.foreach(field => {
      var fieldMap: Map[String, Any] = field.asInstanceOf[Map[String, Any]];

      // type of field
      var typeFiled: String = "";

      // kiểm tra type của field
      if (fieldMap.get("type").get != "struct" &&
        fieldMap.get("type").get != "array" &&
        fieldMap.get("name").get != "_id") {
        primaryKey = fieldMap.get("name").get.toString;

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

  def dropIndex[T](xs: List[T], n: Int) = {
    val (l1, l2) = xs splitAt n
    l1 ::: (l2 drop 1)
  }
}
