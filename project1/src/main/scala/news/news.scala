package news

import java.time.{Instant, Duration, ZoneId}
import java.time.temporal.ChronoField
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
    var inputID = ""
    var inputPW = ""
    var loginLoop = true
    
    //adminMenu() //Skip login for testing

    //Login Menu
    
    do{
      println("Welcome to the News Analyzer, please choose an option:\n" +
        "(1) Login\n" +
        "(0) Exit Application")

      try {  
        var selection = readInt()
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
          "(2) Change Username\n" +
          "(3) Change Password\n" +
          "(0) Logout")
        try {  
          selection = readInt()
          selection match {
            case 1 => {
              analysisMenu()
            }

            case 2 => {
              changeUsername(inputID)
            }

            case 3 => {
              changePassword(inputID)
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
          "(1) Answer Analysis Questions\n" +
          "(2) Change Username\n" +
          "(3) Change Password\n" +
          "(4) Get Data from NewsAPI\n" +
          "(5) Get Other Sports News results (for question 4)\n" +
          "(6) Create Necessary Tables\n" +
          "(0) Logout")
        try {  
          selection = readInt()
          selection match {
            case 1 => {
              analysisMenu()
            }

            case 2 => {
              changeUsername(inputID)
            }

            case 3 => {
              changePassword(inputID)
            }

            case 4 => {
              try{
                println("Please enter the start date and time (in ISO-8601 instant format) of the news you would like to get: ")
                val inputTo: Instant = Instant.parse(readLine())
                println("Now, enter the end date and time (again in ISO-8601 instant format) of the news you would like to get: ")
                val inputFrom: Instant = Instant.parse(readLine())
                println("Finally, enter the file name that you would like to save the news as (include file extension): ")
                val inputFilename = readLine()
                createFile(inputTo, inputFrom, inputFilename)
              }
              catch {
                case e: Exception => println("Invalid input received (Make sure start/end time are in proper format).")
              }
            }

            case 5 => {
              try{
                println("Please enter the start date and time (in ISO-8601 instant format) of the other news you would like to get.\n" +
                  "This only affects question number 4:")
                val inputTo: Instant = Instant.parse(readLine())
                println("Now, enter the end date and time (again in ISO-8601 instant format) of the news you would like to get.\n" +
                  "This only affects question number 4 (Also, with the developer API key, the max supported time frame is 1 month):")
                val inputFrom: Instant = Instant.parse(readLine())
                createOtherFile(inputTo, inputFrom)
              }
              catch {
                case e: Exception => println("Invalid input received (Make sure start/end time are in proper format).")
              }
            }

            case 6 => {
              println("Creating tables for analysis questions. This only needs to be run once at the start of using the application for the first time.")
              createTables()
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
              answerQuestion1()
            }

            case 2 => {
              answerQuestion2()
            }

            case 3 => {
              answerQuestion3()
            }

            case 4 => {
              answerQuestion4()
            }

            case 5 => {
              answerQuestion5()
            }

            case 6 => {
              answerQuestion6()
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

  val path = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/news/production/nhlnews/"

  case class Article (source: Source, author: String, title: String, description: String, url: String, urlToImage: String, publishedAt: String, content: String)
  case class Source (id: String, name: String)
  case class apiResponse (status: String, totalResults: Int, articles: Array[Article])
  case class errResponse (status: String, code: String, message: String)

  def createFile(fromTime: Instant, toTime: Instant, inputName: String): Unit = {
    val filename = path + inputName
    println(s"Creating file $filename ...")
    
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    
    //Get new from NewsAPI
    implicit val formats = DefaultFormats
    var currentTime = fromTime
    // Check if file exists. If yes, delete it.
    println("Checking if it already exists...")
    val filepath = new Path(filename)
    val isExisting = fs.exists(filepath)
    if(isExisting) {
      println("Yes it does exist. Deleting it...")
      fs.delete(filepath, false)
    }
    val output = fs.create(new Path(filename))
    val writer = new PrintWriter(output)

    while(currentTime.isBefore(toTime)){
      //Get articles from NewsAPI
      implicit val formats = DefaultFormats
      var data = getRestContent(everythingQuery("nhl", currentTime, currentTime.plusSeconds(21599)))
      //println(data)
      try{
      val jValue = parse(data)
      val results = jValue.extract[apiResponse]
      println(results)
      val currentZDT = currentTime.atZone(ZoneId.of("UTC"))
      results.articles.foreach(a => writer.write(s"${currentZDT.getMonthValue()},${currentZDT.getDayOfMonth()},${cleanString(a.title)},${cleanString(a.description)}\n"))
      }
      catch {
        case e: Exception => {
          val jValue = parse(data)
          val results = jValue.extract[errResponse]
          println(results)
        }
      }

      currentTime = currentTime.plus(Duration.ofHours(6))
    }
    writer.close()
    println(s"Done creating file $filename ...")
  }

  def createOtherFile(fromTime: Instant, toTime: Instant): Unit = {
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    // Check if file exists. If yes, delete it.
    //println("Checking if it already exists...")
    val otherFilename = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/news/production/othernews/othernewstotals.csv"
    println(s"Creating file $otherFilename ...")
    val otherFilepath = new Path(otherFilename)
    val otherIsExisting = fs.exists(otherFilepath)
    if(otherIsExisting) {
      //println("Yes it does exist. Deleting it...")
      fs.delete(otherFilepath, false)
    }
    val otherOutput = fs.create(new Path(otherFilename))
    val otherWriter = new PrintWriter(otherOutput)


      //Get articles from NewsAPI for other sports(nba, nfl, mlb) (total hits is all that is needed)
      implicit val formats = DefaultFormats
      var nhlData = getRestContent(everythingQuery("nhl", fromTime, toTime))
      var nbaData = getRestContent(everythingQuery("nba", fromTime, toTime))
      var nflData = getRestContent(everythingQuery("nfl", fromTime, toTime))
      var mlbData = getRestContent(everythingQuery("mlb", fromTime, toTime))
      //println(data)
      try{
      val jValue = parse(nhlData)
      val results = jValue.extract[apiResponse]
      println(results)
      otherWriter.write(s"nhl,${results.totalResults}\n")
      }
      catch {
        case e: Exception => {
          val jValue = parse(nbaData)
          val results = jValue.extract[errResponse]
          println(results)
        }
      }
      try{
      val jValue = parse(nbaData)
      val results = jValue.extract[apiResponse]
      println(results)
      otherWriter.write(s"nba,${results.totalResults}\n")
      }
      catch {
        case e: Exception => {
          val jValue = parse(nbaData)
          val results = jValue.extract[errResponse]
          println(results)
        }
      }
      try{
      val jValue = parse(nflData)
      val results = jValue.extract[apiResponse]
      println(results)
      otherWriter.write(s"nfl,${results.totalResults}\n")
      }
      catch {
        case e: Exception => {
          val jValue = parse(nflData)
          val results = jValue.extract[errResponse]
          println(results)
        }
      }
      try{
      val jValue = parse(mlbData)
      val results = jValue.extract[apiResponse]
      println(results)
      otherWriter.write(s"mlb,${results.totalResults}\n")
      }
      catch {
        case e: Exception => {
          val jValue = parse(mlbData)
          val results = jValue.extract[errResponse]
          println(results)
        }
      }

    otherWriter.close()

  }

  def cleanString(input: String): String = {
    //Gets rid of punctuation and undesirable content in article titles/description such as html tags (<li>, <b>, etc.). Also trims input.
    return input.replace("<ol>", "").replace("</ol>", " ").replace("</li>"," ").replace("<li>", "").replace("<b>", "").replace("</b>", "").replace(",", "").replace(".", "").replace("\n", " ").replace("\t", " ").replace("’", " ").replace("'", "").replace("\"", "").replace("!", "").replace("?", "").replace("`", "").replace(";", "").replace(":", "").replace("(", "").replace(")", "").trim()
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

  def createTables(): Unit = {
    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/newsprod";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      //NHL news table data located in HDFS at /user/maria_dev/news/production/nhlnews/
      statement.execute("create external table if not exists nhlnews(month int, day int, title string, description string) row format delimited fields terminated by ',' location '/user/maria_dev/news/production/nhlnews'")
      //Other news table data located in HDFS at /user/maria_dev/news/production/othernews/
      statement.execute("create external table if not exists othernews(sport string, day int) row format delimited fields terminated by ',' location '/user/maria_dev/news/production/othernews'")
      println("Tables created.")
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

  def login(username: String, password: String): Array[Boolean] = {

    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/loginDB";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      var loginResults = statement.executeQuery("SELECT * FROM credentials WHERE username = '" + username + "'")
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

  def changeUsername(username: String): Unit = {
    println(s"Changing username: $username")
    println("Input new username, or enter 0 to cancel: ")
    val newUsername = readLine()
    var password = ""
    var admin = false
    if(!{newUsername.equals("0")}){
      val conf = new Configuration()
      val fs = FileSystem.get(conf)
      // Check if file exists. If yes, user already exists, exit
      //println("Checking if it already exists...")
      val newFilename = s"hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/news/credentials/$newUsername.csv"
      //println(s"Creating file $filename ...")
      val newFilepath = new Path(newFilename)
      val newisExisting = fs.exists(newFilepath)
      if(newisExisting) {
        println("That username already exists, please try again with a different username.")
      }
      else{
        try{
          var driverName = "org.apache.hive.jdbc.HiveDriver"
          val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/loginDB";
          Class.forName(driverName);

          con = DriverManager.getConnection(conStr, "", "");
          val statement = con.createStatement();
          println("Updating username, please wait.")
          var results = statement.executeQuery(s"select password, admin from credentials where username = '$username'")
          if(results.next()){
            password = results.getString(1)
            admin = results.getBoolean(2)
          }
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

        // Check if file exists. If yes, delete it.
        //println("Checking if it already exists...")
        val filename = s"hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/news/credentials/$username.csv"
        //println(s"Creating file $filename ...")
        val filepath = new Path(filename)
        val isExisting = fs.exists(filepath)
        if(isExisting) {
          //println("Yes it does exist. Deleting it...")
          fs.delete(filepath, false)
        }
        val output = fs.create(new Path(newFilename))
        val writer = new PrintWriter(output)
        try{
          writer.write(s"$newUsername,$password,$admin")
        }
        catch{
          case e: Exception => println("Error Changing Username.")
        }
        writer.close()
        println(s"Username changed to: $newUsername")
      }
    }
  }

  def changePassword(username: String): Unit = {
    var admin = false
    println(s"Changing password for user: $username")
    println("Input new password, or enter 0 to cancel: ")
    val newPassword1 = readLine()
    if(!{newPassword1.equals("0")}){
      println("Re-input password: ")
      val newPassword2 = readLine()
      if (newPassword1 != newPassword2) println("Passwords did not match, please try again.")
      else {
        try{
          var driverName = "org.apache.hive.jdbc.HiveDriver"
          val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/loginDB";
          Class.forName(driverName);

          con = DriverManager.getConnection(conStr, "", "");
          val statement = con.createStatement();
          println("Changing password, please wait.")
          var results = statement.executeQuery(s"select admin from credentials where username = '$username'")
          if(results.next()){
            admin = results.getBoolean(1)
          }
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

        val conf = new Configuration()
        val fs = FileSystem.get(conf)
        // Check if file exists. If yes, delete it.
        //println("Checking if it already exists...")
        val filename = s"hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/news/credentials/$username.csv"
        //println(s"Creating file $filename ...")
        val filepath = new Path(filename)
        val isExisting = fs.exists(filepath)
        if(isExisting) {
          //println("Yes it does exist. Deleting it...")
          fs.delete(filepath, false)
        }
        val output = fs.create(new Path(filename))
        val writer = new PrintWriter(output)
        try{
          writer.write(s"$username,$newPassword1,$admin")
        }
        catch{
          case e: Exception => println("Error Changing Password.")
        }
        writer.close()
        println("Password changed.")
      }
    }
  }

  def answerQuestion1(): Unit = {
    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/newsprod";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      println("Running query on Hive, please wait.")
      var results = statement.executeQuery("select month, day, count(day) from nhlnews group by month, day")
      while(results.next()){
        println(s"Month: ${results.getInt(1)}\tDay: ${results.getInt(2)}\tNumber of Articles: ${results.getInt(3)}")
      }
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

  def answerQuestion2(): Unit = {
    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/newsprod";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      println("Running query on Hive, please wait.")
      var results = statement.executeQuery("select 'Carolina Hurricanes:', count(*) as count from nhlnews where upper(title) rlike 'HURRICANE' or upper(description) rlike 'HURRICANE' union all " +
        "select 'Columbus Blue Jackets:', count(*) as count from nhlnews where upper(title) rlike 'BLUE JACKET' or upper(description) rlike 'BLUE JACKET' union all " +
        "select 'New Jersey Devils:', count(*) as count from nhlnews where upper(title) rlike ' DEVILS ' or upper(description) rlike ' DEVILS ' union all " +
        "select 'New York Islanders:', count(*) as count from nhlnews where upper(title) rlike 'ISLANDER' or upper(description) rlike 'ISLANDER' union all " +
        "select 'New York Rangers:', count(*) as count from nhlnews where upper(title) rlike ' RANGERS ' or upper(description) rlike ' RANGERS ' union all " +
        "select 'Philadelphia Flyers:', count(*) as count from nhlnews where upper(title) rlike 'FLYER' or upper(description) rlike 'FLYER' union all " +
        "select 'Pittsburgh Penguins:', count(*) as count from nhlnews where upper(title) rlike 'PENGUIN' or upper(description) rlike 'PENGUIN' union all " +
        "select 'Washington Capitals:', count(*) as count from nhlnews where upper(title) rlike 'CAPITAL' or upper(description) rlike 'CAPITAL' union all " +
        "select 'Boston Bruins:', count(*) as count from nhlnews where upper(title) rlike 'BRUIN' or upper(description) rlike 'BRUIN' union all " +
        "select 'Buffalo Sabres:', count(*) as count from nhlnews where upper(title) rlike 'SABRE' or upper(description) rlike 'SABRE' union all " +
        "select 'Detroit Red Wings:', count(*) as count from nhlnews where upper(title) rlike 'RED WING' or upper(description) rlike 'RED WING' union all " +
        "select 'Florida Panthers:', count(*) as count from nhlnews where upper(title) rlike 'PANTHER' or upper(description) rlike 'PANTHER' union all " +
        "select 'Montreal Canadiens:', count(*) as count from nhlnews where upper(title) rlike 'CANADIENS' or upper(description) rlike 'CANADIENS' union all " +
        "select 'Ottawa Senators:', count(*) as count from nhlnews where upper(title) rlike 'SENATORS' or upper(description) rlike 'SENATORS' union all " +
        "select 'Tampa Bay Lightning:', count(*) as count from nhlnews where upper(title) rlike 'LIGHTNING' or upper(description) rlike 'LIGHTNING' union all " +
        "select 'Toronto Maple Leafs:', count(*) as count from nhlnews where upper(title) rlike 'MAPLE LEA' or upper(description) rlike 'MAPLE LEA' union all " +
        "select 'Arizona Coyotes:', count(*) as count from nhlnews where upper(title) rlike 'COYOTE' or upper(description) rlike 'COYOTE' union all " +
        "select 'Dallas Stars:', count(*) as count from nhlnews where upper(title) rlike 'DALLAS' and upper(title) rlike 'STARS' or upper(description) rlike 'DALLAS' and upper(description) rlike 'STARS' union all " +
        "select 'Colorado Avalanche:', count(*) as count from nhlnews where upper(title) rlike 'AVALANCHE' or upper(description) rlike 'AVALANCHE' union all " +
        "select 'Chicago Blackhawks:', count(*) as count from nhlnews where upper(title) rlike 'BLACKHAWKS' or upper(description) rlike 'BLACKHAWKS' union all " +
        "select 'Minnesota Wild:', count(*) as count from nhlnews where upper(title) rlike ' WILD ' or upper(description) rlike ' WILD ' union all " +
        "select 'Nashville Predators:', count(*) as count from nhlnews where upper(title) rlike 'PREDATOR' or upper(description) rlike 'PREDATOR' union all " +
        "select 'St. Louis Blues:', count(*) as count from nhlnews where upper(title) rlike ' BLUES ' or upper(description) rlike ' BLUES ' union all " +
        "select 'Winnipeg Jets:', count(*) as count from nhlnews where upper(title) rlike 'JETS' or upper(description) rlike 'JETS' union all " +
        "select 'Anaheim Ducks:', count(*) as count from nhlnews where upper(title) rlike 'DUCKS' or upper(description) rlike 'DUCKS' union all " +
        "select 'Calgary Flames:', count(*) as count from nhlnews where upper(title) rlike 'FLAMES' or upper(description) rlike 'FLAMES' union all " +
        "select 'Edmonton Oilers:', count(*) as count from nhlnews where upper(title) rlike ' OILERS ' or upper(description) rlike ' OILERS ' union all " +
        "select 'Los Angeles Kings:', count(*) as count from nhlnews where upper(title) rlike ' KINGS ' or upper(description) rlike ' KINGS ' union all " +
        "select 'San Jose Sharks:', count(*) as count from nhlnews where upper(title) rlike ' SHARKS ' or upper(description) rlike ' SHARKS ' union all " +
        "select 'Seattle Kraken:', count(*) as count from nhlnews where upper(title) rlike 'KRAKEN' or upper(description) rlike 'KRAKEN' union all " +
        "select 'Vancouver Canucks:', count(*) as count from nhlnews where upper(title) rlike 'CANUCK' or upper(description) rlike 'CANUCK' union all " +
        "select 'Vegas Golden Knights:', count(*) as count from nhlnews where upper(title) rlike 'KNIGHTS' or upper(description) rlike 'KNIGHTS' sort by count desc")

      while(results.next){
        println(s"${results.getString(1)}\t${results.getString(2)}")
      }
      
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

  def answerQuestion3(): Unit = {
    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/newsprod";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      println("Running query on Hive, please wait.")
      var results = statement.executeQuery("select 'McDavid: ', count(*) as count from nhlnews where upper(title) rlike 'MCDAVID' or upper(description) rlike 'MCDAVID' union all " +
        "select 'Ovechkin: ', count(*) as count from nhlnews where upper(title) rlike 'OVECHKIN' or upper(description) rlike 'OVECHKIN' union all " +
        "select 'Crosby: ', count(*) as count from nhlnews where upper(title) rlike 'CROSBY' or upper(description) rlike 'CROSBY' sort by count desc")
      while(results.next()){
        println(results.getString(1) + "\t" + results.getString(2))
      }
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

  def answerQuestion4(): Unit = {
    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/newsprod";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      println("Running query on Hive, please wait.")
      var results = statement.executeQuery("select * from othernews")
      while(results.next()){
        println(s"Sport: ${results.getString(1)}\tNumber of Articles: ${results.getInt(2)}")
      }
      
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

  def answerQuestion5(): Unit = {
    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/newsprod";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      println("Running query on Hive, please wait.")
      var results = statement.executeQuery("select 'M', count(*) as count from nhlnews where upper(title) rlike 'HURRICANE' or upper(description) rlike 'HURRICANE' union all " +
        "select 'M', count(*) as count from nhlnews where upper(title) rlike 'BLUE JACKET' or upper(description) rlike 'BLUE JACKET' union all " +
        "select 'M', count(*) as count from nhlnews where upper(title) rlike ' DEVILS ' or upper(description) rlike ' DEVILS ' union all " +
        "select 'M', count(*) as count from nhlnews where upper(title) rlike 'ISLANDER' or upper(description) rlike 'ISLANDER' union all " +
        "select 'M', count(*) as count from nhlnews where upper(title) rlike ' RANGERS ' or upper(description) rlike ' RANGERS ' union all " +
        "select 'M', count(*) as count from nhlnews where upper(title) rlike 'FLYER' or upper(description) rlike 'FLYER' union all " +
        "select 'M', count(*) as count from nhlnews where upper(title) rlike 'PENGUIN' or upper(description) rlike 'PENGUIN' union all " +
        "select 'M', count(*) as count from nhlnews where upper(title) rlike 'CAPITAL' or upper(description) rlike 'CAPITAL' union all " +
        "select 'A', count(*) as count from nhlnews where upper(title) rlike 'BRUIN' or upper(description) rlike 'BRUIN' union all " +
        "select 'A', count(*) as count from nhlnews where upper(title) rlike 'SABRE' or upper(description) rlike 'SABRE' union all " +
        "select 'A', count(*) as count from nhlnews where upper(title) rlike 'RED WING' or upper(description) rlike 'RED WING' union all " +
        "select 'A', count(*) as count from nhlnews where upper(title) rlike 'PANTHER' or upper(description) rlike 'PANTHER' union all " +
        "select 'A', count(*) as count from nhlnews where upper(title) rlike 'CANADIENS' or upper(description) rlike 'CANADIENS' union all " +
        "select 'A', count(*) as count from nhlnews where upper(title) rlike 'SENATORS' or upper(description) rlike 'SENATORS' union all " +
        "select 'A', count(*) as count from nhlnews where upper(title) rlike 'LIGHTNING' or upper(description) rlike 'LIGHTNING' union all " +
        "select 'A', count(*) as count from nhlnews where upper(title) rlike 'MAPLE LEA' or upper(description) rlike 'MAPLE LEA' union all " +
        "select 'C', count(*) as count from nhlnews where upper(title) rlike 'COYOTE' or upper(description) rlike 'COYOTE' union all " +
        "select 'C', count(*) as count from nhlnews where upper(title) rlike 'DALLAS' and upper(title) rlike 'STARS' or upper(description) rlike 'DALLAS' and upper(description) rlike 'STARS' union all " +
        "select 'C', count(*) as count from nhlnews where upper(title) rlike 'AVALANCHE' or upper(description) rlike 'AVALANCHE' union all " +
        "select 'C', count(*) as count from nhlnews where upper(title) rlike 'BLACKHAWKS' or upper(description) rlike 'BLACKHAWKS' union all " +
        "select 'C', count(*) as count from nhlnews where upper(title) rlike ' WILD ' or upper(description) rlike ' WILD ' union all " +
        "select 'C', count(*) as count from nhlnews where upper(title) rlike 'PREDATOR' or upper(description) rlike 'PREDATOR' union all " +
        "select 'C', count(*) as count from nhlnews where upper(title) rlike ' BLUES ' or upper(description) rlike ' BLUES ' union all " +
        "select 'C', count(*) as count from nhlnews where upper(title) rlike 'JETS' or upper(description) rlike 'JETS' union all " +
        "select 'P', count(*) as count from nhlnews where upper(title) rlike 'DUCKS' or upper(description) rlike 'DUCKS' union all " +
        "select 'P', count(*) as count from nhlnews where upper(title) rlike 'FLAMES' or upper(description) rlike 'FLAMES' union all " +
        "select 'P', count(*) as count from nhlnews where upper(title) rlike ' OILERS ' or upper(description) rlike ' OILERS ' union all " +
        "select 'P', count(*) as count from nhlnews where upper(title) rlike ' KINGS ' or upper(description) rlike ' KINGS ' union all " +
        "select 'P', count(*) as count from nhlnews where upper(title) rlike ' SHARKS ' or upper(description) rlike ' SHARKS ' union all " +
        "select 'P', count(*) as count from nhlnews where upper(title) rlike 'KRAKEN' or upper(description) rlike 'KRAKEN' union all " +
        "select 'P', count(*) as count from nhlnews where upper(title) rlike 'CANUCK' or upper(description) rlike 'CANUCK' union all " +
        "select 'P', count(*) as count from nhlnews where upper(title) rlike 'KNIGHTS' or upper(description) rlike 'KNIGHTS'")

      var mcount = 0
      var acount = 0
      var ccount = 0
      var pcount = 0
      while(results.next()){
        {results.getString(1)} match {
          case "M" => mcount += results.getInt(2)
          case "A" => acount += results.getInt(2)
          case "C" => ccount += results.getInt(2)
          case "P" => pcount += results.getInt(2)
        }
      }

      val counts = List((mcount, "Metropolitan"), (acount, "Atlantic"), (ccount, "Central"), (pcount, "Pacific"))
      val sortedCounts = counts.sortWith(_._1 > _._1)
      sortedCounts.foreach {
        case(count, division) => {
          println(s"Division: $division:\tArticles: $count")
        }
      }
      
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

  def answerQuestion6(): Unit = {
    try{
      var driverName = "org.apache.hive.jdbc.HiveDriver"
      val conStr = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/newsprod";
      Class.forName(driverName);

      con = DriverManager.getConnection(conStr, "", "");
      val statement = con.createStatement();
      println("Running query on Hive, please wait.")
      var results = statement.executeQuery("select month, day, count(*) from nhlnews where upper(title) rlike 'KRAKEN' or upper(description) rlike 'KRAKEN' group by month, day")
      while (results.next()){
        println(s"Month: ${results.getString(1)}\tDay: ${results.getString(2)}\tArticles: ${results.getString(3)}")
      }
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