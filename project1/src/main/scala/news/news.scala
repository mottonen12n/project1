package news

import java.time.Instant
import scala.io.StdIn._
import java.io.IOException
import java.sql.{SQLException, Connection, ResultSet, Statement, DriverManager}
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import net.liftweb.json._

object NewsMain {
  
  def main(args: Array[String]): Unit = {
    //Variables
    var credentials:Map[String, String] = Map("nmottonen" -> "password", "mrbasic" -> "password", "admin" -> "Password")
    var admins:List[String] = List("nmottonen", "admin")
    var inputID = ""
    var inputPW = ""
    var loginLoop = true
    //Login
    do{
      println("Welcome to the News Analyzer, please enter your user ID:")
      inputID = readLine()
      println(s"Enter password for $inputID:")
      inputPW = readLine()
      if(inputPW == credentials(inputID)){
        if(admins.contains(inputID)) {
          loginLoop = false
          adminMenu()
        }
        else {
          loginLoop = false
          basicMenu()
        }
      } else println("Invalid username or password, please try again.")
    } while(loginLoop)
    //Get News API key from system environment variable 'NEWS_API_KEY'
    //Option(System.getenv(NewsApiKeyEnv)) match {
    //  case Some(apiKey) => getNews(apiKey, fromDate, toDate)
    //  case None =>
    //    throw new RuntimeException(s"Please provide a valid api key as $NewsApiKeyEnv")
    //}
    def basicMenu(){
      //Variables
      var selection = -1
      var basicLoop = true
      //Main menu
      do{
        println("Welcome to the news analyzer, please make a selection:\n" +
          "(1)\n" +
          "(2)\n" +
          "(3)\n" +
          "(0) Exit Application")
        try {  
          selection = readInt()
          selection match {
            case 1 => {
              println("You selected 1")
              createFile()
            }

            case 2 => {
              println("You selected 2")
            }

            case 3 => {
              println("You selected 3")
            }
            case 0 => {
              basicLoop = false
            }
            case _ => {
              println("Invalid selection, please try again.")
            }
          }
        }
        catch {
          case e: Exception => println("Invalid input, please try again.")
        }
      } while(basicLoop)
    }

    def adminMenu(){
      //Variables
      var selection = -1
      var adminLoop = true
      //Main menu
      do{
        println("Welcome to the news analyzer admin menu, please make a selection:\n" +
          "(1)\n" +
          "(2)\n" +
          "(3)\n" +
          "(0) Exit Application")
        try {  
          selection = readInt()
          selection match {
            case 1 => {
              println("You selected 1")
              createFile()
            }

            case 2 => {
              println("You selected 2")
            }

            case 3 => {
              println("You selected 3")
            }
            case 0 => {
              adminLoop = false
            }
            case _ => {
              println("Invalid selection, please try again.")
            }
          }
        }
        catch {
          case e: Exception => println("Invalid input, please try again.")
        }
      } while(adminLoop)
    } 
  }

  val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"
  val apiKey = "514d7d8f72a14c57a8e1a70db84bc052"
  var fromDate = Instant.parse("2021-10-05T00:00:00Z")
  var toDate = Instant.now()

  case class Article (source: Source, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
  case class Source (id: String, name: String)
  case class apiResponse (status: String, totalResults: Int, articles: Array[Article])

  def getNews(){
  implicit val formats = DefaultFormats
        var data = getRestContent("https://newsapi.org/v2/everything?q=nhl&apiKey=514d7d8f72a14c57a8e1a70db84bc052")
        //println(data)
        val jValue = parse(data)
        val results = jValue.extract[apiResponse]
        println(results)
        for(a <- results.articles) println(s"${a.title} | ${a.source.name} | ${a.publishedAt} | ${a.content}")
  }

  def copyFromLocal(): Unit = {
    val src = "file:///home/maria_dev/files2.txt"
    val target = path + "files2.txt"
    println(s"Copying local file $src to $target ...")
    
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val localpath = new Path(src)
    val hdfspath = new Path(target)
    
    fs.copyFromLocalFile(false, localpath, hdfspath)
    println(s"Done copying local file $src to $target ...")
  }

  def createFile(): Unit = {
    val filename = path + "nhlnews.csv"
    println(s"Creating file $filename ...")
    
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    
    //Get new from NewsAPI
    implicit val formats = DefaultFormats
    var data = getRestContent("https://newsapi.org/v2/everything?q=nhl&apiKey=514d7d8f72a14c57a8e1a70db84bc052")
    //println(data)
    val jValue = parse(data)
    val results = jValue.extract[apiResponse]
    println(results)
    //for(a <- results.articles) println(s"${a.title} | ${a.source.name} | ${a.publishedAt} | ${a.content}")

    // Check if file exists. If yes, delete it.
    println("Checking if it already exists...")
    val filepath = new Path( filename)
    val isExisting = fs.exists(filepath)
    if(isExisting) {
      println("Yes it does exist. Deleting it...")
      fs.delete(filepath, false)
    }

    val output = fs.create(new Path(filename))
    
    val writer = new PrintWriter(output)
    results.articles.foreach(a => writer.write(s"${a.publishedAt},${a.source.name},${a.title},${a.description}\n"))
    writer.close()
    
    println(s"Done creating file $filename ...")
  }

  def getRestContent(url: String): String = {
    val httpClient = new DefaultHttpClient()
    val httpResponse = httpClient.execute(new HttpGet(url))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    httpClient.getConnectionManager().shutdown()
    return content
  }

}