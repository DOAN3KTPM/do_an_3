package controllers

import java.util

import javax.inject._

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
import scala.util.parsing.json.JSON.parseFull

import play.api.libs.json.JsValue
import play.api.i18n.I18nSupport
import play.api._
import scala.collection.JavaConversions._
import play.api.mvc._


@Singleton
class MetaDataController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {

  // Form
  import play.api.data.Forms._
  import play.api.data.Form
  import WidgetForm._

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


    var metaData: DatabaseMetaData = connection.getMetaData();


    // metadata
    //    var resultSet: ResultSet = metaData.getColumns(null, null, "posts", null);
    //    while (resultSet.next()) {
    //      var name = resultSet.getString("COLUMN_NAME");
    //      var `type` = resultSet.getString("TYPE_NAME");
    //      var size = resultSet.getInt("COLUMN_SIZE");
    //
    //      System.out.println("Column name: [" + name + "]; type: [" + `type` + "]; size: [" + size + "]");
    //    }


    //    var foreignKeys: ResultSet  = metaData.getImportedKeys(connection.getCatalog(), null, "posts");
    //
    //
    //    var tablesa: ResultSet = metaData.getPrimaryKeys(null, null, "posts");
    //
    //    while (tablesa.next()) {
    //      var columnName = tablesa.getString("COLUMN_NAME");
    //      println("getPrimaryKeys(): columnName=" + columnName);
    //    }
    //   while (foreignKeys.next()) {
    //
    //     println("---------------------------2")
    //     println(foreignKeys.getString("PKTABLE_NAME")); // foreign table name
    //     println(foreignKeys.getString("PKCOLUMN_NAME")); // primary key of foreign table
    //     println("---------------------------3")
    //
    //
    //
    //     println("---------------------------6")
    //     println(foreignKeys.getString("FKTABLE_NAME")); // table name
    //     println(foreignKeys.getString("FKCOLUMN_NAME")); // foreign key
    //
    //
    //    }
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

  def getMetaDataMYSQL() = Action(parse.tolerantFormUrlEncoded) { request =>
    var tables = List[Table]();
    var hostname = request.body.get("hostname").map(_.head).get;

    var port = request.body.get("port").map(_.head).get;
    var dbname = request.body.get("dbname").map(_.head).get;
    val url: String = "jdbc:mysql://" + hostname + ":" + port + "/" + dbname;
    val driver = "com.mysql.jdbc.Driver";
    val username = request.body.get("username").map(_.head).get;
    val password = request.body.get("password").map(_.head).get;
    val tables_input = parseFull(request.body.get("tables_input")
      .map(_.head).get).get.asInstanceOf[List[String]]
    Class.forName(driver);

    val connection = DriverManager.getConnection(url, username, password);

    var metaData: DatabaseMetaData = connection.getMetaData();

    for (table_name <- tables_input) {
      var fields: List[Field] = List[Field]();

      // get pk
      var primary_key = "";
      var pk_table: ResultSet = metaData.getPrimaryKeys(null, null, table_name);

      while (pk_table.next()) {
        primary_key = pk_table.getString("COLUMN_NAME");
      } // end get pk

      // get metadata
      var resultSet: ResultSet = metaData.getColumns(null, null, table_name, null);
      while (resultSet.next()) {
        var name = resultSet.getString("COLUMN_NAME");
        var `type` = resultSet.getString("TYPE_NAME");
        var size = resultSet.getInt("COLUMN_SIZE");
        var nullable = resultSet.getString("NULLABLE");

        val field = Field(name, `type`, nullable, Length(size, 0), RelationshipField("", ""))
        fields = field :: fields
      } // end get metadata


      var foreignKeys: ResultSet = metaData.getImportedKeys(connection.getCatalog(), null, table_name);

      while (foreignKeys.next()) {
//        println("---------------------------2")
//        println(foreignKeys.getString("PKTABLE_NAME")); // foreign table name
//        println(foreignKeys.getString("PKCOLUMN_NAME")); // primary key of foreign table
//        println("---------------------------3")
//
//
//        println("---------------------------6")
//        println(foreignKeys.getString("FKTABLE_NAME")); // table name
//        println(foreignKeys.getString("FKCOLUMN_NAME")); // foreign key

        fields.map((field: Field) => {
          var foreign_key = foreignKeys.getString("FKCOLUMN_NAME")
          var foreign_table = foreignKeys.getString("PKTABLE_NAME")
          var primary_key_foreign_table = foreignKeys.getString("PKCOLUMN_NAME")
          if(field.name == foreign_key) {
            field.relationship = RelationshipField(foreign_table, primary_key_foreign_table)
          }
        })
      }
      var table = Table(table_name, primary_key, fields, "")
      tables = table :: tables
    }



    var message: String = "Successful connection";
    var status: Int = 200;
    Ok(Json.obj("status" -> "200", "message" -> "Success", "tables" -> Json.toJson(tables)))
  }

}


