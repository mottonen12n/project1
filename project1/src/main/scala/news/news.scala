package news

import com.neovisionaries.i18n.CountryCode
import com.github.fedeoasi.newsapi._
import java.time.Instant
import scala.io.StdIn._
import java.io.IOException

object NewsMain {
  def main(args: Array[String]): Unit = {
    //Variables
    val NewsApiKeyEnv = "NEWS_API_KEY"
    var fromDate = Instant.parse("2021-10-05T00:00:00Z")
    var toDate = Instant.now()
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
      var selection = -1;
      //Main menu
      println("Welcome to the news analyzer, please make a selection:\n" +
        "(1)\n" +
        "(2)\n" +
        "(3)")
      try {  
        selection = readInt()
        selection match {
          case 1 => {
            println("You selected 1")
            //Get News API key from system environment variable 'NEWS_API_KEY'
            Option(System.getenv(NewsApiKeyEnv)) match {
              case Some(apiKey) => getNews(apiKey, fromDate, toDate)
              case None =>
              throw new RuntimeException(s"Please provide a valid api key as $NewsApiKeyEnv")
            }
            
          }

          case 2 => {
            println("You selected 2")
          }

          case 3 => {
            println("You selected 3")
          }
        }
      }
      catch {
        case e: Exception => println("Invalid input, please try again.")
      }
    }

    def adminMenu(){
      //Variables
      var selection = -1;
      //Main menu
      println("Welcome to the news analyzer admin menu, please make a selection:\n" +
        "(1)\n" +
        "(2)\n" +
        "(3)")
      selection = readInt()
      selection match {
        case 1 => {
          println("You selected 1")
          //Get News API key from system environment variable 'NEWS_API_KEY'
          Option(System.getenv(NewsApiKeyEnv)) match {
            case Some(apiKey) => getNews(apiKey, fromDate, toDate)
            case None =>
            throw new RuntimeException(s"Please provide a valid api key as $NewsApiKeyEnv")
          }
          
        }

        case 2 => {
          println("You selected 2")
        }

        case 3 => {
          println("You selected 3")
        }
      }   
    }
  }

  def getNews(apiKey: String, fromDate: Instant, toDate:Instant){
    val client = NewsApiClient(apiKey)
    val pageS = 100
    val Right(response) = client.everything(query = "nhl", from = Some(fromDate), to = Some(toDate), pageSize = Some(pageS))
    println(s"Found ${response.totalResults} articles.")
    response.articles.foreach(a => println(s"${a.publishedAt} - ${a.source.name} - ${a.title}"))
    
  }


}