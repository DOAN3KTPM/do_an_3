package controllers

import play.api.libs.json.JsValue
import play.api.libs.json._

import javax.inject._
import play.api.i18n.I18nSupport

import javax.inject._
import play.api._
import play.api.mvc._
import java.sql.DriverManager
import java.sql.Connection
import scala.runtime.ScalaRunTime._
import play.api.data._
import play.api.data.Forms._
import play.api.data.validation.Constraints._
import play.api.mvc.Session

//import Conn._
/**
  * This controller creates an `Action` to handle HTTP requests to the
  * application's home page.
  */
@Singleton
class HomeController @Inject()(cc: ControllerComponents) extends AbstractController(cc) with I18nSupport {

  // Form
  import play.api.data.Forms._
  import play.api.data.Form
  import WidgetForm._


  private var url: String = _;
  private var driver: String = "com.mysql.jdbc.Driver";
  private var username: String = "root";
  private var password: String = "";

  private var tables: Array[String] = Array[String]();

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

    Ok(views.html.index(tables, form))
  }

  def getMetaTable() = Action { implicit request =>
    //
    //
    //    val url: String = "jdbc:mysql://localhost:3306/web_man_fashion";
    //    val driver = "com.mysql.jdbc.Driver";
    //    val username = "root";
    //    val password = "";
    var columns: Array[Map[String, String]] = Array[Map[String, String]]();
    try {

      Class.forName(driver);
      val connection = DriverManager.getConnection(url, username, password);

      val selected_table: String = request.body.asFormUrlEncoded.get("table")(0);

      var statement = connection.createStatement;
      var rs = statement.executeQuery(s"DESCRIBE   ${selected_table}");


      while (rs.next) {
        val name = rs.getString(1);
        val data_type = rs.getString(2);
        val null_col = rs.getString(3);
        val key = rs.getString(4);
        val default = rs.getString(5);
        val extra = rs.getString(6);

        val metadata: Map[String, String] = Map(
          "name" -> name,
          "data_type" -> data_type,
          "null_col" -> null_col,
          "key" -> key,
          "default" -> default,
          "extra" -> extra
        );

        columns = columns :+ metadata
      }
    }

    Ok(Json.toJson(columns))

  }

  // This will be the action that handles our form post
  def connect = Action { implicit request =>

    val errorFunction = { formWithErrors: Form[Data] =>

      BadRequest(views.html.index(tables, form))
    }

    val successFunction = { data: Data =>
      // This is the good case, where the form was successfully parsed as a Data object.


      url = "jdbc:mysql://" + data.hostname + ":" + data.port + "/" + data.database;
      username = data.username;
      password = data.password;
      Class.forName(driver);
      val connection = DriverManager.getConnection(url, username, password);

      var statement = connection.createStatement;

      var rs = statement.executeQuery("SHOW TABLES");
      var tables: Array[String] = Array[String]();
      while (rs.next) {
        val tableName = rs.getString(1);
        tables = tables :+ tableName;
      }


      Ok(views.html.table(tables))
    }

    val formValidationResult = form.bindFromRequest
    formValidationResult.fold(errorFunction, successFunction)
  }


}
