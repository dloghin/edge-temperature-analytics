import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import collection.JavaConversions._

val inputFile = "../../data/temp-sg.txt"
val outputPath = "../../data/spark-out"

def parseJSON(line : String) : Iterable[(String, (Double, Int))] = {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  val map1 = mapper.readTree(line)
  return map1.get("items").get(0).get("readings").map(rec => (rec.get("station_id").asText, (rec.get("value").asDouble, 1)))
}

val lines = Source.fromFile(inputFile).getLines.toList
val sensors = lines.flatMap(line => parseJSON(line))
val sensorsRDD = sc.parallelize(sensors)
val averagesRDD = sensorsRDD.reduceByKey((a,b) => (a._1+b._1,a._2+b._2)).map(rec => (rec._1, rec._2._1/rec._2._2))

// print
averagesRDD.collect().foreach(println)

// save
averagesRDD.saveAsTextFile(outputPath)

// The code below does not work due to some serialization exception
// sc.textFile(inputFile).flatMap(line => parseJSON2(line))
