import  org.apache.avro.ipc.specific.Person
import scala.tools.nsc.doc.model.Public
import SparkBigData.session_spark

object Launch{

/* the Developer signature and the project topic */
private val bigData_branch : String = "Streaming Pipeline with Kafka"

  class Person(fName:String, lName:String, location:String)

  def main(args: Array[String]): Unit = {
    println("This is a large scale streaming data pipeline with Kafka")

    var test: Int = 15
    test = test + 10
  }

  val ss = session_spark(true)
  val df = ss.read.csv("C:\\Users\\stach\\Downloads\\DimEmployee.csv")
  df.show()
  //df.write.format("csv").save("C:/Users/stach/IdeaProjects/project_streaming/spark-warehouse/echantillon")
}

