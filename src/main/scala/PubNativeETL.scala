package PubNative

import java.io.{File, StringWriter}

import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

trait Mapper {

  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
}

object PubNativeETL extends Mapper {

  private lazy val writer = new StringWriter()

  def parseJSONintoList(stringJSON: String): List[Map[String, Object]] = mapper.readValue[List[Map[String, Object]]](stringJSON)

  def convertToJSON(result: Any): ArrayNode = {
    val root = mapper.createObjectNode()
    val jsonList: ArrayNode = mapper.valueToTree(result)
    root.putArray("list").addAll(jsonList)
  }

  def write(result: ArrayNode, path: String): Unit = {
    mapper.writeValue(new File(path), result)
  }

}
