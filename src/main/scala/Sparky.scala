import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.io._
import scala.io.StdIn.readLine
import scala.util.control.Breaks._

object Sparky {
  var curUser = ""
  var passwd = ""
  var cate = ""
  var times = ""

  def RefPastGames(util:SparkSession): Unit ={
    val html = Source.fromURL("https://statsapi.web.nhl.com/api/v1/teams?expand=team.schedule.previous")
    val s = html.mkString
    val resu = s.split("\\\n")

    var cur = ""
    var tote = ""

    var idtemp = ""
    var gampk = ""
    var awaid = 0
    var homid = 0
    var date = ""
    var aScore = 0
    var hScore = 0

    resu.foreach(e=> {
      val f = e.trim

      if (f.contains("\"gamePk\"")) {
        gampk = f.split(":")(1).trim
        gampk = gampk.substring(0, gampk.length - 1)
      } else if (f.contains("\"date\" : ")){
        date = f.split(":")(1).trim
        date = date.substring(1, date.length - 2)
      } else if (f == "\"away\" : {") {
        cur = "away"
      } else if (f == "\"home\" : {") {
        cur = "home"
      } else if(f == "}, {"||f.contains("venue")) {
        cur = ""
      }

      if (cur=="away"){
        if (f.contains("\"score\"")){
          idtemp = f.split(":")(1).trim
          aScore = idtemp.substring(0,idtemp.length-1).toInt
        } else if (f.contains("\"id\"")) {
          idtemp = f.split(":")(1).trim
          awaid = idtemp.substring(0,idtemp.length-1).toInt
        }
      } else if (cur=="home"){
        if (f.contains("\"score\"")){
          idtemp = f.split(":")(1).trim
          hScore = idtemp.substring(0,idtemp.length-1).toInt
        } else if (f.contains("\"id\"")) {
          idtemp = f.split(":")(1).trim
          homid = idtemp.substring(0,idtemp.length-1).toInt

          if(tote!=""){
            tote+=s"\n$gampk,$awaid,$aScore,$homid,$hScore,$date"
          } else {
            tote+=s"$gampk,$awaid,$aScore,$homid,$hScore,$date"
          }
        }
      }
    })

    val file = new File("pastGamesFile.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(tote)
    bw.close()

    util.sql("DROP TABLE IF EXISTS pastGames")
    util.sql("CREATE TABLE pastGames(GameID String,AwayID int,AwayScore int,HomeID int,HomeScore int,Date string) row format delimited fields terminated by ','")
    util.sql("LOAD DATA LOCAL INPATH 'pastGamesFile.csv' INTO TABLE pastGames")
  }
  def RefNextGames(util:SparkSession): Unit = {
    val html = Source.fromURL("https://statsapi.web.nhl.com/api/v1/teams?expand=team.schedule.next")
    val s = html.mkString
    val resu = s.split("\\\n")

    var cur = ""
    var tote = ""


    var idtemp = ""
    var tempT = ""

    var gampk = ""
    var awaid = 0
    var homid = 0
    var date = ""
    var est = ""
    var cst = ""
    var mst = ""
    var pst = ""

    resu.foreach(e=> {
      val f = e.trim

      if (f.contains("\"gamePk\"")) {
        gampk = f.split(":")(1).trim
        gampk = gampk.substring(0, gampk.length - 1)
      } else if (f.contains("\"date\" : ")) {
        date = f.split(":")(1).trim
        date = date.substring(1, date.length - 2)
      } else if (f.contains("\"gameDate\" :")) {
        tempT = f.split("T")(1) // 23:00:00Z",
        tempT = tempT.substring(0, tempT.length - 3) // 23:00:00
        var holdT = tempT.split(":") // [23] [00] [00]
        if (holdT(0).toInt < 4) holdT(0) = (holdT(0).toInt + 24).toString
        est = s"${holdT(0).toInt - 4}:${holdT(1)}"
        if (holdT(0).toInt < 5) holdT(0) = (holdT(0).toInt + 24).toString
        cst = s"${holdT(0).toInt - 5}:${holdT(1)}"
        if (holdT(0).toInt < 6) holdT(0) = (holdT(0).toInt + 24).toString
        mst = s"${holdT(0).toInt - 6}:${holdT(1)}"
        if (holdT(0).toInt < 7) holdT(0) = (holdT(0).toInt + 24).toString
        pst = s"${holdT(0).toInt - 7}:${holdT(1)}"
      } else if (f == "\"away\" : {") {
        cur = "away"
      } else if (f == "\"home\" : {") {
        cur = "home"
      } else if(f == "}, {"||f.contains("venue")) {
        cur = ""
      }

      if (cur=="away") {
        if(f.contains("\"id\"")){
          idtemp = f.split(":")(1).trim
          awaid = idtemp.substring(0,idtemp.length-1).toInt
        }
      }else if (cur=="home") {
        if(f.contains("\"id\"")){
          idtemp = f.split(":")(1).trim
          homid = idtemp.substring(0,idtemp.length-1).toInt

          if(tote!=""){
            tote+=s"\n$gampk,$awaid,$homid,$date,$est,$cst,$mst,$pst"
          }else{
            tote+=s"$gampk,$awaid,$homid,$date,$est,$cst,$mst,$pst"
          }
        }
      }
      //println(cur)
    })

    val file = new File("nextGamesFile.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(tote)
    bw.close()

    util.sql("DROP TABLE IF EXISTS nextGames")
    util.sql("CREATE TABLE nextGames(GameID String,AwayID int,HomeID int,Date String,EST string,CST string,MST string,PST string) row format delimited fields terminated by ','")
    util.sql("LOAD DATA LOCAL INPATH 'nextGamesFile.csv' INTO TABLE nextGames")
  }
  def RefRoster(util:SparkSession): Unit ={
    val html = Source.fromURL("https://statsapi.web.nhl.com/api/v1/teams?expand=team.roster")
    val s = html.mkString
    val resu = s.split("\\\n")

    var cur = ""
    var tote = ""

    var idtemp = ""
    var teamid = 0
    var playid = 0
    var nameFull = ""
    var nameF = ""
    var nameL = ""
    var jersey = "0"
    var position = ""

    resu.foreach(e=>{
      val f = e.trim

      if(f=="\"teams\" : [ {"||f=="}, {") {
        cur = "team"
      }else{
        if(f.contains(": {")) {
          cur = f.split(":")(0).trim
        }
      }

      cur match {
        case "team" => {
          if(f.contains("\"id\"")){
            idtemp = f.split(":")(1).trim
            teamid = idtemp.substring(0,idtemp.length-1).toInt
          }
        }
        case "\"person\"" => {
          if(f.contains("\"id\"")){
            idtemp = f.split(":")(1).trim
            playid = idtemp.substring(0,idtemp.length-1).toInt
          }else if(f.contains("\"fullName\"")){
            nameFull = f.split(":")(1).trim
            nameFull = nameFull.substring(1,nameFull.length-2)
          }else if(f.contains("\"jerseyNumber\"")){
            jersey = f.split(":")(1).trim
            jersey = jersey.substring(1,jersey.length-2)
          }
        }
        case "\"position\"" => {
          if(f.contains("\"name\"")){
            position = f.split(":")(1).trim
            position = position.substring(1,position.length-2)

            if(tote!=""){
              tote+=s"\n$playid,$teamid,$nameFull,$position,$jersey"
            } else {
              tote+=s"$playid,$teamid,$nameFull,$position,$jersey"
            }
          }
        }
        case _ => // do nothing
      }
    })

    val file = new File("playersFile.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(tote)
    bw.close()

    util.sql("DROP TABLE IF EXISTS players")
    util.sql("CREATE TABLE players(ID int,TeamID int,FullName String,Position String,JerseyNumber String) row format delimited fields terminated by ','")
    util.sql("LOAD DATA LOCAL INPATH 'playersFile.csv' INTO TABLE players")
  }
  def RefTeams(util:SparkSession): Unit ={
    val html = Source.fromURL("https://statsapi.web.nhl.com/api/v1/teams")
    val s = html.mkString
    val resu = s.split("\\\n")

    var cur = ""
    var tote = ""

    var name = ""
    var idtemp = ""
    var id = 0
    var abb = ""
    var tName = ""
    var lName = ""


    resu.foreach(e=>{
      val f = e.trim
      if(f=="\"teams\" : [ {"||f=="}, {") {
        cur = "team"
      }else{
        if(f.contains(": {")) {
          cur = f.split(":")(0).trim
        }
      }

      if(cur=="team"){
        if(f.contains("\"id\"")) {
          idtemp = f.split(":")(1).trim
          id = idtemp.substring(0,idtemp.length-1).toInt
        } else if(f.contains("\"name\"")) {
          name = f.split(":")(1).trim
          name = name.substring(1,name.length-2)
        } else if(f.contains("\"link\"")) {
          //println(s"$id, $name")
        }
      } else {
        if(f.contains("\"abbreviation\"")){
          abb = f.split(":")(1).trim
          abb = abb.substring(1,abb.length-2)
        } else if(f.contains("\"teamName\"")){
          tName = f.split(":")(1).trim
          tName = tName.substring(1,tName.length-2)
        } else if(f.contains("\"locationName\"")){
          lName = f.split(":")(1).trim
          lName = lName.substring(1,lName.length-2)

          if(tote!=""){
            tote+=s"\n$id,$name,$abb,$lName,$tName"
          }else{
            tote+=s"$id,$name,$abb,$lName,$tName"
          }

        }
      }
      // println(e)

    })

    val file = new File("teamsFile.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(tote)
    bw.close()

    util.sql("DROP TABLE IF EXISTS teams")
    util.sql("CREATE TABLE teams(ID Int,Team String,ABB String,Location String,Name String) row format delimited fields terminated by ','")
    util.sql("LOAD DATA LOCAL INPATH 'teamsFile.csv' INTO TABLE teams")
  }
  def RefStandings(util:SparkSession): Unit ={
    val html = Source.fromURL("https://statsapi.web.nhl.com/api/v1/teams?expand=team.stats")
    val s = html.mkString
    val resu = s.split("\\\n")

    var cur = ""
    var tote = ""

    var idtemp = ""
    var id = 0
    var conf = ""
    var div = ""
    var gp = 0
    var w = 0
    var l = 0
    var otl = 0
    var pts = 0
    var gpg = 0.0
    var gapg = 0.0

    resu.foreach(e=>{
      val f = e.trim
      if(f.contains("\"team\"")&&cur=="stat"){
        cur = "team"
      }else if(f.contains("\"division\"")){
        cur = "division"
      }else if(f.contains("\"conference\"")){
        cur = "conference"
      }else if(f.contains("\"splits\"")){
        cur = "stat"
      }else if(f.contains("}, {")){
        cur = ""
      }

      cur match {
        case "division" => {
          if(f.contains("\"name\"")){
            div = f.split(":")(1).trim
            div = div.substring(1,div.length-2)
          }
        }
        case "conference" => {
          if(f.contains("\"name\"")){
            conf = f.split(":")(1).trim
            conf = conf.substring(1,conf.length-2)
          }
        }
        case "stat" => {
          if(f.contains("\"gamesPlayed\"")){
            idtemp = f.split(":")(1).trim
            gp = idtemp.substring(0,idtemp.length-1).toInt
          }else if(f.contains("\"wins\"")){
            idtemp = f.split(":")(1).trim
            w = idtemp.substring(0,idtemp.length-1).toInt
          }else if(f.contains("\"losses\"")){
            idtemp = f.split(":")(1).trim
            l = idtemp.substring(0,idtemp.length-1).toInt
          }else if(f.contains("\"ot\"")){
            idtemp = f.split(":")(1).trim
            otl = idtemp.substring(0,idtemp.length-1).toInt
          }else if(f.contains("\"pts\"")){
            idtemp = f.split(":")(1).trim
            pts = idtemp.substring(0,idtemp.length-1).toInt
          }else if(f.contains("\"goalsPerGame\"")){
            idtemp = f.split(":")(1).trim
            gpg = idtemp.substring(0,idtemp.length-1).toDouble
          }else if(f.contains("\"goalsAgainstPerGame\"")){
            idtemp = f.split(":")(1).trim
            gapg = idtemp.substring(0,idtemp.length-1).toDouble
          }
        }
        case "team" => {
          if(f.contains("\"id\"")){
            idtemp = f.split(":")(1).trim
            id = idtemp.substring(0,idtemp.length-1).toInt

            if(tote!=""){
              tote+=s"\n$id,$conf,$div,$gp,$w,$l,$otl,$pts,$gpg,$gapg"
            }else{
              tote+=s"$id,$conf,$div,$gp,$w,$l,$otl,$pts,$gpg,$gapg"
            }
          }
        }
        case _ => // do nothing
      }
    })

    val file = new File("standingsFile.csv")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(tote)
    bw.close()

    util.sql("DROP TABLE IF EXISTS standings")
    util.sql("CREATE TABLE standings(TeamID int,Conference String,Division String,GP int,W int,L int,OTL int,PTS int,GPG double,GAPG double) row format delimited fields terminated by ','")
    util.sql("LOAD DATA LOCAL INPATH 'standingsFile.csv' INTO TABLE standings")
  }

  def Refresher(util:SparkSession,cate:String): Unit ={
    cate match {
      case "teams" => RefTeams(util)
      case "next game" => RefNextGames(util)
      case "last game" => RefPastGames(util)
      case "roster" => RefRoster(util)
      case "standings" => RefStandings(util)
      case "all" => {
        RefTeams(util)
        RefRoster(util)
        //RefNextGames(util)
        // This is commented out because I don't want to refresh this until presentation day.
        // To demonstrate that the program auto-updates information when requested to.
        RefPastGames(util)
        RefStandings(util)
      }
    }
  }

  /*
  TODO:
    Refresher takes a parameter of each table type.
    Refresher also takes a parameter of our spark session
    Redownloads and updates the table requested in our spark session

    What kind of tables?
    General Teams (ID,Team,ABB,Loc,Name)
    Next Game (TeamID,Home or Away,Opponent)
    Past Game (TeamID,Score,Home or Away,Score,Opponent)
    Roster (TeamID,PlayerID,First Name,Last Name, if it gives position, also that)
    Standings (TeamID,Wins,Losses,OTL,Points)
   */

  def Login(util:SparkSession): Unit ={
    var user = ""
    var pass = ""
    var valid = false

    do {
      println("Username;")
      user = readLine
      println("Password;")
      pass = readLine

      val users = util.table("users")
      val check = users.select("Username", "Password").collect()
      breakable {
        check.foreach(e => {
          if (e.toString == s"[${user.toLowerCase},$pass]") {
            valid = true
            break
          }
        })
      }

      if(valid==false) {
        println("Username or Password is incorrect. Try again.")
      }else{
        val skritt = users.filter(users("Username").equalTo(user.toLowerCase)).select("UserType","Timezone").collect()(0).toString
        val tick = skritt.split(",")
        cate = tick(0).substring(1)
        times = tick(1).substring(0,tick(1).length-1)
      }
    } while(valid==false)
    curUser = user
    passwd = pass
  }
  def NewUser(util:SparkSession,check:Boolean): Unit ={
    var user = ""
    var pass = ""
    var topy = ""
    var tz = ""
    // Username, Password, Type, Timezone
    println("Create Username;")
    user = readLine
    println("Create password;")
    pass = readLine

    do {
      println("Basic or Admin User?")
      readLine.toLowerCase match {
        case "basic" => topy = "basic"
        case "admin" => topy = "admin"
        case _ => println("Unclear. Try again.")
      }
    } while(topy=="")

    do {
      println("Your timezone?")
      println("[EST | CST | MST | PST]")
      readLine.toLowerCase match {
        case "est" => tz = "est"
        case "cst" => tz = "cst"
        case "mst" => tz = "mst"
        case "pst" => tz = "pst"
        case _ => println("Unclear. Try again.")
      }
    } while(tz=="")

    println("Verifying...")
    if(check){
      var valid = true
      do {
        // check the table
        val users = util.table("users")
        val check = users.select("Username").collect()
        valid = true
        breakable {
          check.foreach(e => {
            if (e.toString == s"[${user.toLowerCase}]") {
              valid = false
              break
            }
          })
        }
        if (valid) { // means username is acceptable
          util.sql("INSERT INTO users VALUES " +
            s"(\'${user.toLowerCase}\',\'$pass\',\'$topy\',\'$tz\')")
        } else {
          println("Username is taken. Please insert a new username.")
          user = readLine
        }
      } while (!valid)
    }else {
      // no table? Make one, put us in.
      util.sql("CREATE TABLE users(Username String,Password String,UserType String,Timezone String)")
      util.sql("INSERT INTO users VALUES " +
        s"(\'${user.toLowerCase}\',\'$pass\',\'$topy\',\'$tz\')")
    }
    curUser = user
    passwd = pass
    cate = topy
    times = tz
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      //.config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      // will enable later
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //spark.sql("DROP TABLE IF EXISTS users")

    //spark.sql("SELECT * FROM users").show(false)

    println("[Login | Register]")
    readLine.toLowerCase match {
      case "register" => {
        NewUser(spark,spark.catalog.tableExists("users"))
      }
      case "login" => {
        Login(spark)
      }
      case _ => println("idk that lmao")
    }

    println(s"$curUser,$passwd,$cate,$times")

    //val tempDF = spark.table("teams")
    //tempDF.show(32,false)
    // important to keep around ^^

  //  Refresher(spark,"all")
  /*  spark.sql("SELECT teams.Team, teams.ABB, teams.Location, teams.Name FROM teams").show(32,false)
    // spark.sql("SELECT * FROM players").show(900, false)
    spark.sql("SELECT players.FullName, players.Position, players.JerseyNumber, teams.Team FROM players INNER JOIN teams ON players.TeamID = teams.ID").show(false)
    spark.sql("SELECT DISTINCT nextGames.Date, teams.Team as Away, teams2.Team as Home, nextGames.est as Time FROM nextGames " +
      "INNER JOIN teams ON nextGames.AwayID == teams.ID " +
      "INNER JOIN teams as teams2 ON nextGames.HomeID == teams2.ID " +
      "ORDER BY nextGames.Date").show(false)
    // yes, this is messy.
    // No, there's not a better way to do it.
    spark.sql("SELECT DISTINCT pastGames.Date, teams.Team as Away, pastGames.AwayScore, teams2.Team as Home, pastGames.HomeScore FROM pastGames " +
      "INNER JOIN teams ON pastGames.AwayID == teams.ID " +
      "INNER JOIN teams as teams2 ON pastGames.HomeID == teams2.ID " +
      "ORDER BY pastGames.Date").show(false)
    spark.sql("SELECT teams.Team, standings.Conference, standings.Division, standings.GP, standings.W, standings.L, standings.OTL, standings.PTS, " +
      "standings.GPG, standings.GAPG FROM standings " +
      "INNER JOIN teams ON standings.TeamID == teams.ID ORDER BY standings.PTS DESC").show(32,false) */
  // THE ABOVE IS LIKE THIS BECAUSE I AM NOW WORKING ON IMPLEMENTING THE INTERACTION SYSTEM


  }
}
