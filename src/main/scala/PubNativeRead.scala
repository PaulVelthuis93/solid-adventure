package PubNative

import java.io.{File}

import scala.io.Source

object PubNativeRead {


  /**
    * @param inputDirectory directory pointing to the input files
    * @return a list with a mapping the click contents, and a list with a mapping containing the impressions contents
    */
  def readClicksAndImpressions(inputDirectory: String): (List[List[Map[String, Object]]], List[List[Map[String, Object]]]) = {
    val clicks = readClicks(inputDirectory + "clicks/")
//    Map the list into click impression_id and revenue
    val clicksList = clicks.map(clickFile => PubNativeETL.parseJSONintoList(clickFile))
    val impressions = readImpressions(inputDirectory + "impressions/")
    val impressionsList = impressions.map(impressionFile => PubNativeETL.parseJSONintoList(impressionFile))
    (clicksList, impressionsList)
  }

  /**
    *
    * @param clicksInputDir the input directory where the clicks are stored
    * @return a list with all the files containing click events
    */
  private def readClicks(clicksInputDir: String): List[String] = {
    val files = getListOfFiles(clicksInputDir)
    files.map(getFileAsString)
  }

  /**
    * @param impressionsInputDir the input directory where the impressions are stored
    *  @return a list with all the files containing impression events
    */
  private def readImpressions(impressionsInputDir: String): List[String] = {
    val files = getListOfFiles(impressionsInputDir)
    files.map(getFileAsString)
  }

  private def getFileAsString(file: File): String = Source.fromFile(file.getPath).getLines().mkString

  /**
    * @param dirPath the directory in which the files should be listed
    * @return a list of files in a specific directory
    */
  private def getListOfFiles(dirPath: String): List[File] = {
    val directory = new File(dirPath)
    if (directory.exists() && directory.isDirectory) {
      directory.listFiles().filter(_.isFile).toList
    } else List[File]()
  }
}