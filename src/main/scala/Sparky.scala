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
              tote+=s"\n$playid,$nameFull,$position,$jersey,$teamid"
            } else {
              tote+=s"$playid,$nameFull,$position,$jersey,$teamid"
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
    util.sql("CREATE TABLE players(ID int,FullName String,Position String,JerseyNumber String,TeamID int) " +
      "row format delimited fields terminated by ','")
    util.sql("LOAD DATA LOCAL INPATH 'playersFile.csv' INTO TABLE players")
    util.sql("DROP TABLE IF EXISTS partiPlayers")
    util.sql("CREATE TABLE partiPlayers(ID int,FullName String,Position String,JerseyNumber String) PARTITIONED BY (teamid int)")
    util.sql("INSERT INTO TABLE partiPlayers(SELECT * FROM players WHERE " +
      "teamid=24 or teamid=53 or teamid=6 or teamid=7 or teamid=20 or teamid=12 or teamid=16 or teamid=21 or teamid=29 or " +
      "teamid=25 or teamid=17 or teamid=22 or teamid=13 or teamid=26 or teamid=30 or teamid=8 or teamid=18 or teamid=1 or " +
      "teamid=2 or teamid=3 or teamid=9 or teamid=4 or teamid=5 or teamid=28 or teamid=55 or teamid=19 or teamid=14 or teamid=10 or " +
      "teamid=23 or teamid=54 or teamid=15 or teamid=52) ORDER BY players.Position")
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
  def Refresher(util:SparkSession,tirg:String): Unit ={
    tirg match {
      case "teams" => RefTeams(util)
      case "next game" => RefNextGames(util)
      case "last game" => RefPastGames(util)
      case "roster" => RefRoster(util)
      case "standings" => RefStandings(util)
      case "all" => {
        RefTeams(util)
        RefRoster(util)
        //RefNextGames(util)
        RefPastGames(util)
        RefStandings(util)
      }
      case "notNext" => { // debug & testing purposes
        RefTeams(util)
        RefRoster(util)
        RefPastGames(util)
        RefStandings(util)
      }
    }
  }

  def Login(util:SparkSession): Unit ={
    var user = ""
    var pass = ""
    var valid = false

    do {
      println("Username;")
      user = readLine.toLowerCase.filterNot(_.isWhitespace)
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
        val skritt = users.filter(users("Username").equalTo(user)).select("UserType","Timezone").collect()(0).toString
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
    user = readLine.toLowerCase.filterNot(_.isWhitespace)
    do {
      println("Create password;")
      pass = readLine

      if(pass.contains(",")) {
        println("Error, try a different password.")
        pass = ""
      }
    } while (pass=="")

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
            if (e.toString == s"[$user]") {
              valid = false
              break
            }
          })
        }
        if (valid) { // means username is acceptable
          util.sql("INSERT INTO users VALUES " +
            s"(\'$user\',\'$pass\',\'$topy\',\'$tz\')")
        } else {
          println("Username is taken. Please insert a new username.")
          user = readLine.toLowerCase.filterNot(_.isWhitespace)
        }
      } while (!valid)
    }else {
      // no table? Make one, put us in.
      util.sql("CREATE TABLE users(Username String,Password String,UserType String,Timezone String)")
      util.sql("INSERT INTO users VALUES " +
        s"(\'$user\',\'$pass\',\'$topy\',\'$tz\')")
    }
    curUser = user
    passwd = pass
    cate = topy
    times = tz
  }

  def Initial(spark:SparkSession): Unit ={
    do {
      println("[Login | Register | Quit]")
      readLine.toLowerCase match {
        case "register" => {
          NewUser(spark, spark.catalog.tableExists("users"))
          println(s"Welcome, $curUser.")
          InnerLoop(spark,false)
        }
        case "login" => {
          Login(spark)
          println(s"Welcome, $curUser.")
          InnerLoop(spark,false)
        }
        case "quit" | "q" => {
          curUser = "-1"
          InnerLoop(spark,true)
        }
        case _ => println("Input Unclear")
      }
    } while (curUser=="")
  }
  def InnerLoop(spark:SparkSession,tink:Boolean): Unit ={
    var quit = tink
    while (quit==false) {
      println("[Standings | Teams | Players | Games | Refresh | Logout | Quit]")
      readLine.toLowerCase match {
        //case "users" => Users(spark)
        case "standings" => Standings(spark)
        case "teams" => Teams(spark)
        case "refresh" => {
          println("Refreshing the databases takes a bit of time;")
          println("[Standings | Teams | Players | Next Games | Last Games | All | Cancel]")
          readLine.toLowerCase match {
            case "standings" => Refresher(spark,"standings")
            case "teams" => Refresher(spark,"teams")
            case "players" => Refresher(spark,"roster")
           // case "next" | "next games" | "next game" => Refresher(spark,"next game")
            case "last" | "last games" | "last game" => Refresher(spark,"last game")
           // case "all" => Refresher(spark,"all")
            case "debug" => Refresher(spark,"notNext") // refreshes all but next game
            case "cancel" => println("Aborted.")
            case _ => println("Input unclear.")
          }
        }
        case "logout" => {
          quit = true
          Initial(spark)
        }
        case "quit" | "q" => quit = true
        case _ => println("Input unclear.")
      }
    }
  }

  def Teams(util:SparkSession): Unit ={
    println("[All | Specific | Back]")
    readLine.toLowerCase match {
      case "all" => {
        util.sql("SELECT teams.Team, teams.ABB, teams.Location, teams.Name FROM teams").show(32,false)
      }
      case "specific" => {
        println("[Conference | Division]")
        readLine.toLowerCase match {
          case "conference" => {
            println("[Western | Eastern]")
            readLine.toLowerCase match {
              case "western" =>
                util.sql("SELECT teams.Team, teams.ABB, teams.Location, teams.Name, standings.Conference, standings.Division FROM teams " +
                  "INNER JOIN standings ON teams.ID == standings.TeamID WHERE standings.Conference=='Western' ORDER BY standings.Division").show(false)
              case "eastern" =>
                util.sql("SELECT teams.Team, teams.ABB, teams.Location, teams.Name, standings.Conference, standings.Division FROM teams " +
                  "INNER JOIN standings ON teams.ID == standings.TeamID WHERE standings.Conference=='Eastern' ORDER BY standings.Division").show(false)
              case _ => Teams(util)
            }
          }
          case "division" => {
            println("[Atlantic | Metro | Central | Pacific]")
            readLine.toLowerCase match {
              case "atlantic" =>
                util.sql("SELECT teams.Team, teams.ABB, teams.Location, teams.Name, standings.Division FROM teams " +
                  "INNER JOIN standings ON teams.ID == standings.TeamID WHERE standings.Division=='Atlantic'").show(false)
              case "metro" | "metropolitan" =>
                util.sql("SELECT teams.Team, teams.ABB, teams.Location, teams.Name, standings.Division FROM teams " +
                  "INNER JOIN standings ON teams.ID == standings.TeamID WHERE standings.Division=='Metropolitan'").show(false)
              case "central" =>
                util.sql("SELECT teams.Team, teams.ABB, teams.Location, teams.Name, standings.Division FROM teams " +
                  "INNER JOIN standings ON teams.ID == standings.TeamID WHERE standings.Division=='Central'").show(false)
              case "pacific" =>
                util.sql("SELECT teams.Team, teams.ABB, teams.Location, teams.Name, standings.Division FROM teams " +
                  "INNER JOIN standings ON teams.ID == standings.TeamID WHERE standings.Division=='Pacific'").show(false)
              case _ => Teams(util)
            }
          }
          case _ => Teams(util)
        }
      }
      case "back" => // do nothing, we're leaving.
      case _ => Teams(util)
    }
  }
  def Standings(util:SparkSession): Unit ={
    var chicken = ""
    var order = " ORDER BY standings.PTS"
    var noBack = true
    println("[League | Conference | Division | Back]")
    readLine.toLowerCase match {
      case "league" => // carry on.
      case "conference" => {
        println("[Western | Eastern]")
        readLine.toLowerCase match {
          case "western" => chicken = " WHERE standings.Conference=='Western'"
          case "eastern" => chicken = " WHERE standings.Conference=='Eastern'"
          case _ => Standings(util)
        }
      }
      case "division" => {
        println("[Atlantic | Metro | Central | Pacific]")
        readLine.toLowerCase match {
          case "atlantic" => chicken = " WHERE standings.Division=='Atlantic'"
          case "metro" => chicken = " WHERE standings.Division=='Metropolitan'"
          case "central" => chicken = " WHERE standings.Division=='Central'"
          case "pacific" => chicken = " WHERE standings.Division=='Pacific'"
        }
      }
      case "back" => noBack = false
      case _ => Standings(util)
    }

    if (noBack) {
      while (noBack) {
        util.sql("SELECT teams.Team, standings.Conference, standings.Division, standings.GP, standings.W, standings.L, standings.OTL, standings.PTS, " +
          "standings.GPG, standings.GAPG FROM standings " +
          "INNER JOIN teams ON standings.TeamID == teams.ID"+chicken+order+" DESC").show(32,false)
        println("Resort?\n[GP | W | L | OTL | PTS | GPG | GAPG | Back]")
        readLine.toLowerCase match {
          case "gp" => order = " ORDER BY standings.GP"
          case "w" => order = " ORDER BY standings.W"
          case "l" => order = " ORDER BY standings.L"
          case "otl" => order = " ORDER BY standings.OTL"
          case "pts" => order = " ORDER BY standings.PTS"
          case "gpg" => order = " ORDER BY standings.GPG"
          case "gapg" => order = " ORDER BY standings.GAPG"
          case "back" => noBack = false
          case _ => //
        }
      }
    }
  }

  def Users(util:SparkSession): Unit ={
    if(cate=="admin") {
      println("[View | Manage | Back]")
      readLine.toLowerCase match {
        case "view" => {
          println("Enter your password.")
          if(readLine==passwd){
            util.sql("SELECT * FROM users").show(false)
            Users(util)
          } else {
            println("Password incorrect.")
            Users(util)
          }
        }
        case "manage" => {
          println("Enter your password.")
          if(readLine==passwd){
            println("What user to manage?")
            val targ = readLine.toLowerCase.filterNot(_.isWhitespace)
            util.sql(s"SELECT * FROM users WHERE users.Username==\'$targ\'")
            val users = util.table("users")
            val skritt = users.filter(users("Username").equalTo(targ)).select("Password","UserType","Timezone").collect()(0).toString.split(",")
            // password = skritt(0).substring(1)
            // type = skritt(1)
            // timezone = skritt(2).substring(0,skritt(2).length-1)
            println("[Update | Delete | Back]")
            readLine.toLowerCase match {
              case "update" => {
                println("[Password | Type | Timezone]")
                readLine.toLowerCase match {
                  case "password" | "pass" | "passwd" => {
                    var tigg=""
                    do {
                      println("Enter new password;")
                      tigg = readLine
                      if(tigg.contains(",")){
                        println("Error, try a new password.")
                        tigg=""
                      }
                    }while (tigg=="")
                    util.sql(s"DELETE FROM users WHERE user.Username==\'$targ\'")
                    util.sql("INSERT INTO users VALUES " +
                      s"(\'$targ\',\'$tigg\',\'${skritt(1)}\',\'${skritt(2).substring(0,skritt(2).length-1)}\')")
                    println("Updated user.")
                  }
                  case "type" => {
                    var alo = 0
                    if(skritt(1)=="admin"){
                      if(targ!=curUser){
                        println("You cannot demote another admin!")
                      }else{
                        println("Do you want to change your account to basic?")
                        alo = -1
                      }
                    }else{
                      println(s"Do you want to promote $targ to admin? This cannot be undone!")
                      alo = 1
                    }

                    if(alo!=0){
                      println("[Y/N]")
                      if(readLine.toLowerCase=="y"){
                        util.sql(s"DELETE FROM users WHERE user.Username==\'$targ\'")
                        if(alo==1) {
                          util.sql("INSERT INTO users VALUES " +
                            s"(\'$targ\',\'${skritt(0).substring(1)}\',\'admin\',\'${skritt(2).substring(0, skritt(2).length - 1)}\')")
                        }else{
                          util.sql("INSERT INTO users VALUES " +
                            s"(\'$targ\',\'${skritt(0).substring(1)}\',\'basic\',\'${skritt(2).substring(0, skritt(2).length - 1)}\')")
                        }
                        println("Updated user.")
                      } else {
                        println("Aborted.")
                        Users(util)
                      }
                    }
                  }
                  case "timezone" | "time" | "tz" => {
                    var tizzy = ""
                    while (tizzy==""){
                      println("Enter new timezone;")
                      println("[EST | CST | MST | PST]")
                      readLine.toLowerCase match {
                        case "est" => tizzy="est"
                        case "cst" => tizzy="cst"
                        case "mst" => tizzy="mst"
                        case "pst" => tizzy="pst"
                        case _ => println("Unclear. Try again.")
                      }
                      util.sql(s"DELETE FROM users WHERE user.Username==\'$targ\'")
                      util.sql("INSERT INTO users VALUES " +
                        s"(\'$targ\',\'${skritt(0).substring(1)}\',\'${skritt(1)}\',\'$tizzy\')")
                      println("Updated user.")
                    }
                  }
                  case _ => Users(util)
                }
              }
              case "delete" => {
                if(skritt(1)=="admin") {
                  println("Can't delete another admin!")
                  Users(util)
                } else {
                  util.sql(s"DELETE FROM users WHERE users.Username==\'$targ\'")
                  println("User deleted.")
                }
              }
              case _ => Users(util)
            }
          } else {
            println("Password incorrect.")
            Users(util)
          }
        }
        case "back" => // Do nothing, we're leaving.
      }
    }
    // Guess this will just not be accessible because delete and update aren't possible with spark ??????????
  }

  /*
    TEAM IDs:
      Anaheim Ducks - 24
      Arizona Coyotes - 53
      Boston Bruins - 6
      Buffalo Sabres - 7
      Calgary Flames - 20
      Carolina Hurricanes - 12
      Chicago Blackhawks - 16
      Colorado Avalanche - 21
      Columbus Blue Jackets - 29
      Dallas Stars - 25
      Detroit Red Wings - 17
      Edmonton Oilers - 22
      Florida Panthers - 13
      Los Angeles Kings - 26
      Minnesota Wild - 30
      Montreal Canadiens - 8
      Nashville Predators - 18
      New Jersey Devils - 1
      New York Islanders - 2
      New York Rangers - 3
      Ottawa Senators - 9
      Philadelphia Flyers - 4
      Pittsburgh Penguins - 5
      San Jose Sharks - 28
      Seattle Kraken - 55
      St. Louis Blues - 19
      Tampa Bay Lightning - 14
      Toronto Maple Leafs - 10
      Vancouver Canucks - 23
      Vegas Golden Knights - 54
      Washington Capitals - 15
      Winnipeg Jets - 52
   */

  def main(args: Array[String]): Unit = {
    //<editor-fold desc="Spark Setup">
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
    spark.conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    //</editor-fold>

    Initial(spark)

    spark.close()
  /*
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
