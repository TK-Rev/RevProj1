import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.io._

object Sparky {

  
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

  def Refresher(util:SparkSession,cate:String): Unit ={
    cate match {
      case "teams" => RefTeams(util)
      case "next game" => //
      case "last game" => //
      case "roster" => RefRoster(util)
      case "standings" => //
      case "all" => {
        RefTeams(util)
        RefRoster(util)
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

    Refresher(spark,"all")
    spark.sql("SELECT * FROM teams").show()
    // spark.sql("SELECT * FROM players").show(900, false)
    spark.sql("SELECT players.ID, players.FullName, players.Position, players.JerseyNumber, teams.Team FROM players INNER JOIN teams ON players.TeamID = teams.ID").show(900,false)
  }
}
