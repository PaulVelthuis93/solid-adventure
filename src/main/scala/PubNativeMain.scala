
package PubNative


/**
  * In this programming framework we will analyse the ads. These ads are located in the data input folder. There are two kind of ads:
  * clicks: this shows when a user clicks on a certain ad
  * impression: This shows when an advertisement is displayed to a user.  *
  */
object PubNativeMain extends PubNativeSpark {

 def main(args: Array[String]) {
  // Input directory a manual input directory can be given as first argument
  val dir = if (args.isEmpty) "src/main/data/input/" else args (0)
  // Read the content of clicks and impression events
  val (clicklist, impressionlist) = PubNativeRead.readClicksAndImpressions(dir)
//  calculate the metrics:
//  impressions: sum of impressions
//  clicks: sum of clicks
//  revenue: sum of revenue
  val calculatedMetrics = PubNativeTransformer.calculateMetrics(clicklist, impressionlist) // Objective #2
  val result = PubNativeETL.convertToJSON(calculatedMetrics)
//  Store the calculated metrics on defined output path
  PubNativeETL.write(result,if (args.isEmpty) "src/main/data/output/metrics.json" else args(1))


// Reading the metrics to create the recommendations
  val metrics = readJSON(if (args.isEmpty) "src/main/output/metrics.json" else args(1))
  val recommendations = metrics.transform(generateRecommendations)
//  write the recommendations to defined output path
  write(recommendations, if (args.isEmpty)  "data/src/main/output/recommendations.json" else args(2))
  }


}