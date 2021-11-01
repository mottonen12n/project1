package news

import java.time.{Instant, Duration}
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
    
    //adminMenu()

    //Login Menu
    
    do{
      println("Welcome to the News Analyzer, please choose an option:\n" +
        "(1) Login\n" +
        "(0) Exit Application")
      var selection = readInt()
      try {  
        selection match {
          case 1 => {
            println("Enter your username: ")
            inputID = readLine()
            println(s"Enter password for $inputID:")
            inputPW = readLine()
            val checkLogin = login(inputID, inputPW)
            if(checkLogin(0)){
              if(checkLogin(1)) {
                adminMenu()
              }
              else {
                basicMenu()
              }
            } else println("Invalid username or password, please try again.")
          }
          case 0 => {
            loginLoop = false
          }
          case _ => {
            println("Invalid selection, please try again.")
          }
        }
      }
      catch {
        case e: Exception => println("Invalid input, please try again.")
      }
    } while(loginLoop)
    
    def basicMenu(){
      //Variables
      var selection = -1
      var basicLoop = true
      //Main menu
      do{
        println("Welcome to the news analyzer, please make a selection:\n" +
          "(1) Analysis Questions\n" +
          "(2)\n" +
          "(3)\n" +
          "(0) Exit Application")
        try {  
          selection = readInt()
          selection match {
            case 1 => {
              analysisMenu()
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
              analysisMenu()
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
    def analysisMenu(){
      //Variables
      var selection = -1
      var analysisLoop = true
      //Main menu
      do{
        println("Welcome to the analysis questions menu, please make a selection:\n" +
          "(1) Is news about hockey increasing over the last month?\n" +
          "(2) What team(s) have the most news about them?\n" +
          "(3) Which of these popular players shows up in news the most: Alex Ovechkin, Sidney Crosby, or Connor McDavid?\n" +
          "(4) How does volume of news about hockey compare to news about other popular sports in the US, basketball, football, baseball?\n" +
          "(5) What division has the most news about it (Metro, Atlantic, Central, or Pacific)?\n" +
          "(6) How has the NHL's newest team, the Seattle Kraken fared in terms of volume of news over the last month?\n" +
          "(0) Return to Main Menu")
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
              analysisLoop = false
            }
            case _ => {
              println("Invalid selection, please try again.")
            }
          }
        }
        catch {
          case e: Exception => println("Invalid input, please try again.")
        }
      } while(analysisLoop) 
    }
  }

  val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/"

  case class Article (source: Source, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
  case class Source (id: String, name: String)
  case class apiResponse (status: String, totalResults: Int, articles: Array[Article])
  case class errResponse (status: String, code: String, message: String)

  /*def getNews(){
  implicit val formats = DefaultFormats
        var data = getRestContent("https://newsapi.org/v2/everything?q=nhl&apiKey=514d7d8f72a14c57a8e1a70db84bc052")
        //println(data)
        val jValue = parse(data)
        val results = jValue.extract[apiResponse]
        println(results)
        for(a <- results.articles) println(s"${a.title} | ${a.source.name} | ${a.publishedAt} | ${a.content}")
  }*/

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
    val start: Instant = Instant.parse("2021-10-29T00:00:00Z")
    var currentTime = start
    val end: Instant = Instant.parse("2021-10-31T00:00:00Z")
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

    var count = 0
    var day = 29
    while(currentTime.isBefore(end)){
      //Get articles from NewsAPI
      implicit val formats = DefaultFormats
      var data = getRestContent(everythingQuery("nhl", currentTime, currentTime.plusSeconds(28799)))
      //println(data)
      try{
      val jValue = parse(data)
      val results = jValue.extract[apiResponse]
      println(results)
      val dayOfArticle = day + (count/4)
      results.articles.foreach(a => writer.write(s"$dayOfArticle%${a.title}%${a.description}\n"))
      }
      catch {
        case e: Exception => {
          val jValue = parse(data)
          val results = jValue.extract[errResponse]
          println(results)
        }
      }

      currentTime = currentTime.plus(Duration.ofHours(6))
      count += 1
    }
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

  def everythingQuery(query: String, fromDate: Instant, toDate: Instant): String = {
    val base = "https://newsapi.org/v2/everything"
    val apiKeyString = "&apiKey=514d7d8f72a14c57a8e1a70db84bc052"
    val pageSize = "&pageSize=100"
    val language = "&language=en"
    return base + "?q=" + query + "&from=" + fromDate.toString() + "&to=" + toDate.toString() + language + pageSize + apiKeyString
  }

var con: java.sql.Connection = null;
  def login(username: String, password: String): Array[Boolean] = {

    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/loginDB";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      println("connected + statement created")
      statement.execute("create table if not exists loginDB.credentials(username String, password String, admin Boolean) row format delimited fields terminated by ','")
      println("table/db created.")
      var loginResults = statement.executeQuery("SELECT * FROM loginDB.credentials WHERE username = '" + username + "'")
      if(loginResults.next()){
        println(loginResults.getString(1))
        if(loginResults.getString(2) == password) return Array(true, loginResults.getBoolean(3))
        else return Array(false,false)
      } else return Array(false,false)
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace();
        throw new Exception(s"${ex.getMessage}")
      }
    } finally {
      try {
        if (con != null)
          con.close();
      } catch {
        case ex: Exception => {
          ex.printStackTrace();
          throw new Exception(s"${ex.getMessage}")
        }
      }
    }
  }
}