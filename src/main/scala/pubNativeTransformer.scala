package PubNative

import scala.collection.immutable
import scala.collection.immutable.Map

object PubNativeTransformer {

  /**
    * In this step we first reduce the clicks and impressions to count the sum of clicks and impressions
    * The second step is to group them together this is then returned as an iterable map
    * @param clicksList a list containing a mapping of all the clicks
    * @param impressionsList a list containing a mapping of all the impressions
    * @return An iterable map that contains the calculated metrics sum of clicks, sum of impressions, sum of revenue
    */
  def calculateMetrics(clicksList: List[List[Map[String, Object]]], impressionsList: List[List[Map[String, Object]]] ): immutable.Iterable[Map[String, Any]] = {
    val (clicksRenamed, impressions) = reduceAndRenameClicksAndImp(clicksList, impressionsList)
    groupAndJoinClicksAndImpressions(clicksRenamed, impressions)
  }


  /**
    * Reduce the clicks, so that you get  a list containing map of revenue per click and a list containing the impresions reduced per ID)
    * @param clicksList a list containing a mapping of all the clicks
    * @param impressionsList a list containing a mapping of all the impressions
    * @return
    */
  private def reduceAndRenameClicksAndImp(clicksList: List[List[Map[String, Object]]], impressionsList: List[List[Map[String, Object]]]): (List[Map[String, Object]], List[Map[String, Object]]) = {
    val clicks = clicksList.reduce(_ ++ _)
    val clicksRenamed = clicks.map(click => Map("id" -> click("impression_id"), "revenue" -> click("revenue")))
    val impressions = impressionsList.reduce(_ ++ _)
    println(impressions)

    (clicksRenamed, impressions)
  }

  /**
    * Group the impressions and clicks by their ID, resulting in the counted impressions, revenue and clicks, then combine them into a single iterable map
    * @param clicks
    * @param impressions
    * @return resulting in the counted impressions, revenue and clicks, output a single iterable map
    */

  private def groupAndJoinClicksAndImpressions(clicks: List[Map[String, Object]], impressions: List[Map[String, Object]]) = {
    val impressionsGroupped = impressions.groupBy(key => key("id")).mapValues(maps => maps.head ++ Map("impressions" -> maps.size))
    val clicksGroupped = clicks.groupBy(key => key("id")).mapValues(list => Map("revenue" -> list.map(el => el("revenue").asInstanceOf[Double]).sum, "clicks" -> list.size))
    val impWithClicks = impressionsGroupped.toList ++ clicksGroupped.toList
    //  group and map in order to remove the index on which is mapped
    // From List((204c38ac-802e-4558-ac5d-24a0b004a1f9,Map(impressions -> 13, country_code -> US, id -> 204c38ac-802e-4558-ac5d-24a0b004a1f9, advertiser_id -> 31, app_id -> 6))
    val impWithClicksGrouped = impWithClicks.groupBy(_._1).mapValues(_.map(_._2))
    // To Map(204c38ac-802e-4558-ac5d-24a0b004a1f9 -> List(Map(impressions -> 13, country_code -> US, id -> 204c38ac-802e-4558-ac5d-24a0b004a1f9, advertiser_id -> 31, app_id -> 6))
    // Result List(Map(impressions -> 13, country_code -> US, id -> 204c38ac-802e-4558-ac5d-24a0b004a1f9, advertiser_id -> 31, app_id -> 6)
    impWithClicksGrouped.map(_._2.reduce(_ ++ _)) // Merging Maps into one

  }
}



