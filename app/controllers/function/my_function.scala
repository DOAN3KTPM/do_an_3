package function

import java.sql.{DriverManager, ResultSet}

import net.liftweb.json._

object my_function {
  def getElement(elem: String, json:JValue): List[String]  = for {
    JObject(child) <- json
    JField(`elem`, JString(value)) <- child
    // this does not return the expected result
    // JField("username", JString(value)) <- child // this returns the expected result
  } yield  value
  // get all value in json
  def getAllElement(json: JValue) = for{
    JObject(child) <- json
    JField(elem,JString(value)) <- child
  } yield value
  def countElement(regex1 : String, regex2 : String, json : JValue) : Int = {
    /*
        count("fields", "name", json)
        list = getElement("name",json).length = 8
        (jsonData \ "fields")(0) \ "name" -> JString(_id) != JNothing
        => count = 5
        6,7,8 wrong -> catch exceptions
    */
    var count = 0
    var list = getElement(regex2,json).length
    for (i <-0 to list -1){
      try{
        if(((json \ regex1)(i) \ regex2) != JNothing){
          count += 1
        }
      }
      catch {case unknown =>} //do nothing
    }
    return count
  }
  def getElement(json : JValue, regex : String, i : Int) : String ={
    try {
      var value = (json \ "fields") (i) \ regex
      var name = value.toString.replace("JString", "")
        .replaceAll("[(]", "")
        .replaceAll("[)]", "")
      return name
    }
    catch {case unknownError: UnknownError =>}
    return null
  }
  def getName(json : JValue, i : Int) : String = {
    //(jsonData \ "fields"(0) \ "name" -> JString(_id)
    try {
      var value = (json \ "fields") (i) \ "name"
      var name = value.toString.replace("JString", "")
        .replaceAll("[(]", "")
        .replaceAll("[)]", "")
      return name
    }
    catch {case unknownError: UnknownError =>}
    return null
  }
  def getType(json : JValue, i : Int) : String = {
    try{
      var value = (json \ "fields") (i) \ "type"
      var name = value.toString.replace("JString", "")
        .replaceAll("[(]", "")
        .replaceAll("[)]", "")
      if(name.contains(",array,")){
        name = "array"
      }else if(name.contains(",struct,")){
        name = "struct"
      }
      return name
    }
    catch {case unknownError: UnknownError =>}
    return null
  }
  def getChild(regex1 : String, i : Int, regex2 : String, json : JValue) : List[String] = {
    /*(jsonData \ "fields"(3) \\ "name" =>
    JObject(List(JField(name,JString(job)), JField(name,JString(job_name)),
     JField(name,JString(company)), JField(name,JString(company_address))))
     => job,job_name,company,company_address
     */
    var list_value : List[String] = List()
    var value = (json \ regex1)(i) \\ regex2
    var name = value.toString.replaceAll("JString","")
      .replaceAll("JObject","")
      .replaceAll("List","")
      .replaceAll("JField[(]"+regex2+",","")
      .replaceAll("JString","")
      .replaceAll("[(]","")
      .replaceAll("[)]","")
    var array_value = name.split(",")
    for (j<-0 to array_value.length -1){
      list_value = array_value(j).replaceFirst(" ","") :: list_value
    }
    if(list_value.length > 1) {
      return list_value
    }else{
      return null
    }
  }
  def dropIndex[T](xs: List[T], n: Int)  = {
    val (l1, l2) = xs splitAt n
    l1 ::: (l2 drop 1)
  }
  def CheckDBExist(db : String) : Boolean = {
    val conn_str = "jdbc:mysql://localhost:3306/?user=root&password="
    // Load the driver
    classOf[com.mysql.jdbc.Driver]
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)

    try {
      //Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      //Create a Database
      var sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '"+db+"'"
      var result = statement.executeQuery(sql)
      var list_result : List[String] = List()
      while (result.next()){
        list_result = result.getString(1) :: list_result
      }
      if(list_result.isEmpty) {
        return false
      }else return true
    }
    finally {
      // close connect
      conn.close
    }
  }
 def Create_DB(sql : String): Unit ={
    val conn_str = "jdbc:mysql://localhost:3306/?user=root&password="
    // Load the driver
    classOf[com.mysql.jdbc.Driver]
    // Setup the connection
    val conn = DriverManager.getConnection(conn_str)

    try {
      //Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      //Create a Database
      statement.executeUpdate(sql)
    }
    finally {
      // close connect
      conn.close
    }
  }


}
