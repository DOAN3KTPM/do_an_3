//package ScalaDemo
//
//import com.mongodb.casbah.Imports._
//import scala.io.Source
//import net.liftweb.json._
//
//object Unique_Value {
//  var t1 = System.nanoTime()
//  def getElement(elem: String, json:JValue): List[String]  = for {
//    JObject(child) <- json
//    JField(`elem`, JString(value)) <- child
//    // this does not return the expected result
//    // JField("username", JString(value)) <- child // this returns the expected result
//  } yield  value
//  def getName(json : JValue, i : Int) : String = {
//    //(jsonData \ "fields"(0) \ "name" -> JString(_id)
//    try {
//      var value = (json \ "fields") (i) \ "name"
//      var name = value.toString.replace("JString", "")
//        .replaceAll("[(]", "")
//        .replaceAll("[)]", "")
//      return name
//    }
//    catch {case unknownError: UnknownError =>}
//    return null
//  }
//  def getType(json : JValue, i : Int) : String = {
//    try{
//      var value = (json \ "fields") (i) \ "type"
//      var name = value.toString.replace("JString", "")
//        .replaceAll("[(]", "")
//        .replaceAll("[)]", "")
//      if(name.contains(",array,")){
//        name = "array"
//      }else if(name.contains(",struct,")){
//        name = "struct"
//      }
//      return name
//    }
//    catch {case unknownError: UnknownError =>}
//    return null
//  }
//  def countElement(regex1 : String, regex2 : String, json : JValue) : Int = {
//    /*
//        count("fields", "name", json)
//        list = getElement("name",json).length = 8
//        (jsonData \ "fields")(0) \ "name" -> JString(_id) != JNothing
//        => count = 5
//        6,7,8 wrong -> catch exceptions
//    */
//    var count = 0
//    var list = getElement(regex2,json).length
//    for (i <-0 to list -1){
//      try{
//        if(((json \ regex1)(i) \ regex2) != JNothing){
//          count += 1
//        }
//      }
//      catch {case unknown =>} //do nothing
//    }
//    return count
//  }
//  def main(args: Array[String]): Unit = {
//
//    val re :String = "\"type\""
//    val db : String = "test"
//    val collection : String = "book"
//    var database = "\""+db+":"+collection+"\""
//    val a :String = "\"database\" : "+database+",\n\"name\" : \""+collection+"\""+",\n"+"\"type\""
//    val url = db+"."+collection
//    val sparkSession = SparkSession.builder()
//      .master("local")
//      .appName("MongoSpark")
//      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/"+ url)
//      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/"+ url)
//      .getOrCreate()
//    val df = MongoSpark.load(sparkSession)  // Uses the SparkSession
//    val ac = df.schema
//    val ac_tree = ac.treeString.replaceFirst("root", collection)
//    println(ac_tree)
//    var av = ac.json
//    //println(av)
//    val jsonString = ac.prettyJson
//    val json_new = jsonString.replaceFirst(re,a)
////    println(json_new)
////    val pw = new PrintWriter(new File("D:\\DoAn3\\Json\\Output_"+collection+".json" ))
////    pw.write(json_new)
////    pw.close
//
//
////    val lines = Source.fromFile("D:\\DoAn3\\Json\\Output_book.json").mkString
//    var jsonData = json_new
//    var num = countElement("fields", "name", jsonData)
//
//    var list_all_name = getElement("name", jsonData)
//    var list_type = getElement("type", jsonData)
//    var list_new : List[String] = List()
//    var list_name : List[String] = List()
//    for (i <- 0 to num - 1) {
//      list_new = getName(jsonData,i)+":"+getType(jsonData,i) :: list_new
//    }
//    list_new = list_new.filterNot((a:String) => {a.contains("array")})
//    var list_find : List[String] = List()
//    var list_key: List[String] = List()
//    for (i <- 0 to list_new.length - 1){
//      var temp = list_new(i).substring(0,list_new(i).indexOf(":"))
//      if(temp != "_id"){
//        list_find = temp :: list_find
//      }else{
//        list_key = temp :: list_key
//      }
//    }
//    var database = getElement("database",jsonData)
//    val db = database(0).substring(0,database(0).indexOf(":"))
//    val collection = database(0).substring(database(0).indexOf(":")+1,database(0).length)
//    //connect to database
//    val client: MongoClient = MongoClient()
//    val mongoConn = MongoConnection()
//    val mongoColl = mongoConn(db)(collection)
//    val size = mongoColl.size
//    for(i <- 0 to list_find.length -1){
//      var tam = mongoColl.distinct(list_find(i)).size
//      if(tam == size){
//        list_key = list_find(i) :: list_key
//      }
//    }
//    println(list_key)
//
//    var t2 = (System.nanoTime() - t1) / 1e9d
//    println("Time run : "+t2)
//  }
//}