package news

import net.liftweb.json._
import org.apache.http.HttpEntity
import org.apache.http.HttpResponse
import org.apache.http.client.ClientProtocolException
import org.apache.http.client.HttpClient
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.DefaultHttpClient
import java.time.Instant
import java.time.Duration
import scala.io.StdIn._
import java.io.IOException
import java.sql.{SQLException, Connection, ResultSet, Statement, DriverManager}
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.PrintWriter;

object Main {

    case class Article (source: Source, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
    case class Source (id: String, name: String)
    case class apiResponse (status: String, totalResults: Int, articles: Array[Article])
    case class errResponse (status: String, code: String, message: String)

    def main(args: Array[String]): Unit = {
        
      var con: java.sql.Connection = null;
    try {
      // For Hive2:
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/loginDB";

      // For Hive1:
      //var driverName = "org.apache.hadoop.hive.jdbc.HiveDriver"
      //val conStr = "jdbc:hive://sandbox-hdp.hortonworks.com:10000/default";

      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val stmt = con.createStatement();

      println("Executing SELECT * FROM credentials..")
      var res = stmt.executeQuery("SELECT * FROM credentials")
      while (res.next()) {
        println(s"${res.getString(1)}, ${res.getString(2)}")
      }
      
    } catch {
      case ex => {
        ex.printStackTrace();
        throw new Exception(s"${ex.getMessage}")
      }
    } finally {
      try {
        if (con != null)
          con.close();
      } catch {
        case ex => {
          ex.printStackTrace();
          throw new Exception(s"${ex.getMessage}")
        }
      }
    }

      //println(currentTime.toString())
      //var newTime: Instant = currentTime.plus(Duration.ofHours(6))
      //println(newTime)
      //println(everythingQuery("nhl", currentTime, newTime))
        /*implicit val formats = DefaultFormats
        var data = getRestContent("https://newsapi.org/v2/everything?q=nhl&apiKey=514d7d8f72a14c57a8e1a70db84bc052")
        //println(data)
        val jValue = parse(data)
        val results = jValue.extract[apiResponse]
        println(results)
        for(a <- results.articles) println(s"${a.title} | ${a.source.name} | ${a.publishedAt} | ${a.content}")
        */

      /*while(currentTime.isBefore(end)){
        //Get new from NewsAPI
        implicit val formats = DefaultFormats
        var data = getRestContent(everythingQuery("nhl", currentTime, currentTime.plusSeconds(28799)))
        //println(data)
        try{
        val jValue = parse(data)
        val results = jValue.extract[apiResponse]
        println(results)
        }
        catch {
          case e: Exception => {
            val jValue = parse(data)
            val results = jValue.extract[errResponse]
            println(results)
          }
        }
        //for(a <- results.articles) println(s"${a.title} | ${a.source.name} | ${a.publishedAt} | ${a.content}")

        //results.articles.foreach(a => writer.write(s"${a.publishedAt}%${a.title}%${a.description}\n"))
        currentTime = currentTime.plus(Duration.ofHours(6))
      }
      */
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

}