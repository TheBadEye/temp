import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.sql.types._
import spark.sql
import spark.implicits._

import play.api.libs.json.Json

import org.apache.hadoop.fs.{FileSystem, Path}


  var DS = spark.read.
    format("com.databricks.spark.csv").
    option("header", "true").
    option("inferSchema", "true").
    load("SPARKWORKS/me.csv")

  var peopleDS = spark.read.parquet("SPARKWORKS/People.parquet")


  var parPeopleDF = spark.read.parquet("s3a://STORE/data/People.parquet")
  var jsonDF = parPeopleDF.toJSON


  jsonDF.foreach(element =>  
      print(Json.prettyPrint(Json.parse(element)) )
  )


// forced one-liners
 val One = "{\"playerID\":\"zychto01\",\"birthYear\":\"1990\",\"birthMonth\":\"8\",\"birthDay\":\"7\",\"birthCountry\":\"USA\",\"birthState\":\"IL\",\"birthCity\":\"Monee\",\"nameFirst\":\"Tony\",\"nameLast\":\"Zych\",\"nameGiven\":\"Anthony Aaron\",\"weight\":\"190\",\"height\":\"75\",\"bats\":\"R\",\"throws\":\"R\",\"debut\":\"2015-09-04\",\"finalGame\":\"2017-08-19\",\"retroID\":\"zycht001\",\"bbrefID\":\"zychto01\"}"

var OneJSON =  Json.parse(One)
var result: String = Json.prettyPrint(OneJSON)





----------file conversion
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit, udf}

val hadoop = context.hadoopConfiguration
val localDestPath  = new org.apache.hadoop.fs.Path("SPARKWORKS/People.parquet")
val localDestFS    = FileSystem.get(hadoop)
val localFileList  = localDestFS.listFiles(localDestPath, true)
localFileList.foreach(element =>  
      println( element )
  )
 val i: RemoteIterator<LocatedFileStatus> = fs.listFiles(localDestPath, true);
 while (localFileList.hasNext()) {
   println(localFileList.next())
 }

val afsDestPath    = new org.apache.hadoop.fs.Path("s3a://STORE/data/People.parquet")

if (!path.getName.contains("filepart") )
   /**  
   *   This function REMOVES anything in the path sent to it without removing the directory itself
   *  @param inEnvName          example: "hdfs://NNHA"  this determines which cluster we are speaking to 
   *  @param inRootPath       example: "/data/biz/revmgmt/marketshare/database"
   *  @param inDatabaseName     example: "rev_mgmt_analytics" or  val databaseName       = "shaper_tmp"
   *  @param inTableName        example: "dailymarketshare_base" or   val dailyTableName     = "dailymarketshare_base_raw"  or monthlyTableName   = "monthlymarketshare_base" or  val monthlyTableName   = "monthlymarketshare_base_raw"
   *  @return true on success
   **/
  def cleanHDFSDatabasePath(inEnvName: String, inRootPath: String, inDatabaseName: String, inTableName: String): Boolean = { 
    var hdfsLocalLocation = inEnvName + inRootPath + "/" + inDatabaseName + "/" + inTableName
    try {
      shapeJob.changeJobPhase("Persisting Shape", "cleaning path: " + hdfsLocalLocation )
      val fs=FileSystem.get(shapeJob.getSparkSession.sparkContext.hadoopConfiguration)
      val hdfsLocalDestPath  = new org.apache.hadoop.fs.Path(hdfsLocalLocation)
      val hdfsLocalOuputPath = new org.apache.hadoop.fs.Path(hdfsLocalLocation+"/*")
      if(fs.delete(hdfsLocalDestPath,true)){
          // allowed a deletion
          // not go recreate it
          checkHDFSDatabasePath(inEnvName, inRootPath, inDatabaseName, inTableName)
      } else {
        gl.info("Nop'op HivePersistAdaptor.cleanHDFSDatabasePath("+hdfsLocalDestPath+"): path did not exist ... creating")
        checkHDFSDatabasePath(inEnvName, inRootPath, inDatabaseName, inTableName)
      }
    } catch {
      case e: Exception => {
        shapeJob.changeJobPhase("Persisting Shape", "Exception: HivePersistAdaptor.cleanHDFSDatabasePath Failed: " + hdfsLocalLocation + "/*" )
        gl.error("Failed HivePersistAdaptor.cleanHDFSDatabasePath():" + e.getMessage)
        e.printStackTrace()
        return false
      }
    }
    return true
  } //end of func




  -----------------
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus
import com.amazon.deequ.VerificationResult._
import com.amazon.deequ.analyzers.{Analyzer, Histogram, Patterns, State, KLLParameters}


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import spark.sql
import spark.implicits._

import scala.collection.mutable

import javax.script._
import javax.script.ScriptEngine
import javax.script.ScriptEngineManager

import play.api.libs.json.Json

// build up a fake Dataset
var arrayStructData = Seq(
                          Row("James", "K",  "Polk",            "EMPLOYEE", 23, 1),
                          Row("Teddy", "P",  "Pendergrass",     "CLIENT",   53, 2),
                          Row("Pete",  "A",  "BrownPants",      "BOSS",     40, 3),
                          Row("Jim",   "E",  "Tindle",          "CLIENT",   41, 4),
                          Row("Theo",   "F", "VanGuagh",        "CLIENT",   46, 5),
                          Row("Parker", "P", "Posey",           "CLIENT",   44, 6),
                          Row("Pankaj", "P", "Patil",           "CLIENT",   29, 7),
                          Row("Ajana",  "C", "Sathian",         "CLIENT",   24, 8),
                          Row("Ponachamy", "P", "BrownPants",   "CLIENT",   35, 9),
                          Row("Annad",  "G", "Paul",            "CLIENT",   44, 10),
                          Row("Chris",  "B", "Twiner",          "CLIENT",   41, 11),
                          Row("Gover",  "R", "Washington",      "CLIENT",   25, 12)
                         )
// A DataFrame Structure
var arrayStructSchema = StructType( List(StructField("fNAME",StringType,  false ),
                                         StructField("mNAME",StringType,  false ),
                                         StructField("lNAME",StringType,  false ),
                                         StructField("TYPE", StringType,  false ),
                                         StructField("AGE",  IntegerType, false ),
                                         StructField("ID",   IntegerType, false )
                                      )
                                    )
// Build a fake DataFrame to hold the rows we will act like is our Dataset 
var fakeDF = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructData),arrayStructSchema)
// Build a few collection to hold stuff
var rawZoneDQCheckList = scala.collection.mutable.ListBuffer.empty[Check]
var rawZoneResultsDFCheckList = scala.collection.mutable.ListBuffer.empty[DataFrame]
var rawZoneResultsJsonCheckList = scala.collection.mutable.ListBuffer.empty[String]

// DQ Rule Class; we'd build these up by sourcing some DB of the DQRULES and
// their params
case class DQRule(tenant: Option[String], zone: Option[String], funcID: Option[Integer], dqID: Option[String], dqName: Option[String], funcName: Option[String], columnID: Option[String], funcFormat: Option[String], funcStrPayload: Option[String], functIntPayload: Option[Integer]) {

    var dqCheck: com.amazon.deequ.checks.Check = _
    var verifyResults: com.amazon.deequ.VerificationResult = _
    var resultingDataFrame: DataFrame = _
    var resultingJson: String = _

    def buildDQCheck(): Unit = {
      dqCheck = funcID.getOrElse(100) match {
        case 0 => {
          new Check(CheckLevel.Error, dqName.getOrElse("NULL") ).isContainedIn("TYPE", Array("EMPLOYEE", "BOSS", "CLIENT", "OFFICIAL") )
        }
        case 1 => {
          new Check(CheckLevel.Error, dqName.getOrElse("NULL") ).hasSize(_ >= functIntPayload.getOrElse[Integer](12) )
        }
        case 2 => {
          new Check(CheckLevel.Error, dqName.getOrElse("NULL") ).isComplete(funcName.getOrElse("NULL") )
        }
        case 3 => {
          new Check(CheckLevel.Error, dqName.getOrElse("NULL") ).hasMin(columnID.getOrElse("NULL"),_ <= functIntPayload.getOrElse[Integer](20))
        }
        case 4 => {
          new Check(CheckLevel.Error, dqName.getOrElse("NULL") ).hasMax(columnID.getOrElse("NULL"),_ >= functIntPayload.getOrElse[Integer](50))
        }
        case 5 => {
          new Check(CheckLevel.Error, dqName.getOrElse("NULL") ).isUnique(columnID.getOrElse("NULL") )
        }
        case _ => {
          println("Error: no dq function mapping found for :" +funcID )
          new Check(CheckLevel.Error, dqName.getOrElse("NULL") ).hasSize(_ >= 12)
        }
      }
    }

    def runCheck(inFrame: DataFrame ): Unit = {
        verifyResults = VerificationSuite().onData(inFrame).addCheck(dqCheck).run()
        resultingDataFrame = checkResultsAsDataFrame(spark, verifyResults)
        resultingJson = checkResultsAsJson(verifyResults)
        println("DQRule["+dqID.getOrElse("EMPTY")+"].runCheck() executed")
        None
    }
  
    def printJsonResults( ): String = {
        Json.prettyPrint(Json.parse(resultingJson))
    }

    def printDataFrameResults( ): String = {
        resultingDataFrame.show(100, false)
        "Hello"
    }

    def toFormatedString: String = {
       var returnFormated = "-----------DQRule-----------"
            returnFormated+="\n\tTENANT: "+tenant.getOrElse("EMPTY")
            returnFormated+="\n\tZONE: "+zone.getOrElse("EMPTY")
            returnFormated+="\n\tFUNCTION ID: "+funcID.getOrElse("EMPTY")
            returnFormated+="\n\tDQName: "+dqName.getOrElse("EMPTY")
            returnFormated+="\n\tFunction Name: "+funcName.getOrElse("EMPTY")
            returnFormated+="\n\tColumnID: "+columnID.getOrElse("EMPTY")
            returnFormated+="\n\tFunction Format: "+funcFormat.getOrElse("EMPTY")
            returnFormated+="\n\tFunction Str Payload: "+funcStrPayload.getOrElse("EMPTY")
            returnFormated+="\n\tFunction Int Payload: "+functIntPayload.getOrElse("EMPTY")
            returnFormated+="\n----------------------------"
            return returnFormated
   }
}// end of class

// Build of ca collection to hold all the DQRule object we created for this
// run, assume we sourced from MONGO or something
var rawZoneDQSourcesCheckList = new scala.collection.mutable.ListBuffer[DQRule]
// Add some DQ records, they could have come from a query to a jdbc or mongo db json doc store
rawZoneDQSourcesCheckList += new DQRule(Option("CCAR"), Option("RAW"), Option(0), Option("DQ:125"),  Option("Enumerant Test"),        
                                        Option("isContainedIn"),   Option("TYPE"),    Option("$s"),     Option("'EMPLOYEE','BOSS','CLIENT','OFFICIAL'"), Option(null) )
rawZoneDQSourcesCheckList += new DQRule(Option("CCAR"), Option("RAW"), Option(1), Option("DQ:132"),  Option("Row Count test"),        
                                        Option("hasSize"),         Option("RECORDS"), Option("_ >= %d"), Option(null), Option(12) )
rawZoneDQSourcesCheckList += new DQRule(Option("CCAR"), Option("RAW"), Option(2), Option("DQ:342"),  Option("Completeness test"),     
                                        Option("isComplete"),      Option("lNAME"),   Option(null),      Option(null), Option(null))
rawZoneDQSourcesCheckList += new DQRule(Option("CCAR"), Option("RAW"), Option(2), Option("DQ:044"),  Option("Completeness test"),     
                                        Option("isComplete"),      Option("fNAME"),   Option(null),      Option(null), Option(null))
rawZoneDQSourcesCheckList += new DQRule(Option("CCAR"), Option("RAW"), Option(3), Option("DQ:542"),  Option("AGE MIN test"),          
                                        Option("hasMin"),          Option("AGE"),     Option("_ <= %d"), Option(null), Option(23) )
rawZoneDQSourcesCheckList += new DQRule(Option("CCAR"), Option("RAW"), Option(4), Option("DQ:204"),  Option("AGE MAX test"),          
                                        Option("hasMax") ,         Option("AGE"),     Option("_ >= %d"), Option(null), Option(45) )
rawZoneDQSourcesCheckList += new DQRule(Option("CCAR"), Option("RAW"), Option(5), Option("DQ:321"),  Option("PRIM KEY test"),         
                                        Option("isUnique"),        Option("ID"),      Option(null),      Option(null), Option(null) )
// Cycle thru Collection, for each object: build the rule, execute the rule, show json output resulting from execution
for (ruleObject <- rawZoneDQSourcesCheckList ){
    ruleObject.buildDQCheck()
    ruleObject.runCheck(fakeDF)
    println(ruleObject.printJsonResults)
}
