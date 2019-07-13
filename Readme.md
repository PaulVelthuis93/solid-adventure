# PubNative analysing ads

PubNative is a company that helps among others companies to gain insights in their ad spending.
In this programming framework we will analyse the ads. These ads are located in the data input folder. There are two kind of ads:
clicks: this shows when a user clicks on a certain ad
impression: This shows when an advertisement is displayed to a user.

In this code we show three objectives:
1. How to read ads, we want to read the click and impression events all at once
2. How to group ads, we want to do a few aggregations, such as
    impressions: sum of impressions
    clicks: sum of clicks
    revenue: sum of revenue
3. How to make a recommendation for top 5 advertiser_ids to display for app/country combination

## How to run the code
The code can be ran by running the PubNativeMain

There are three configurations that can be given for the main file:
1. The path to the input folder, here is assumed that the clicks, and the impressions each have their separate folder
2. The output path to where the metrics should go
3. The output path to where the recommendations should go

## Requirements
Before building such a framework there are a few requirements.
1. The application should accept list of files with clicks and impression events. There could be up to 200 files with each event type (clicks or impressions)
2. The reading of the files should be done in the first step, so we read them all at once
3. For the first two objective no programming engine may be used like Spark


## Assumptions made
1. For the identification of clicks and impression events we have organised them based on their input folder clicks and impressions.
    It could be that in the real world the data will be listed in one directory, then instead of reading by directory the files should be read via their name.
2. The reading of the data is done in memory, when dealing with huge amount of data it is better to read the files one by one, since the current approach will fail. Also a better processing model like spark could be used.
3. Reading JSON in Java or Scala is challenging, and many people try to reinvent the wheel.
This should be avoided at all costs. For this reason it assumed that a JSON parser may be used. The JSON parser used for this project is Jackson
https://github.com/FasterXML/jackson
