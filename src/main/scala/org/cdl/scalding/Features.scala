package org.cdl.scalding

import com.twitter.scalding._


class Features(args : Args) extends Job(args) {

  def dirs = Map("d" -> "-debug.csv", "p" -> "-prod.csv")

  // input args
  val trainHistoryFile = args("trainHistory")
  val testHistoryFile = args("testHistory")
  val transactionsFile = args("transactions")
  val offersFile = args("offers")

  // csv files
  val train = Csv(trainHistoryFile, separator = ",", skipHeader = true)
  val test = Csv(testHistoryFile, separator = ",",  skipHeader=true)
  val transactions = Csv(transactionsFile, separator = ",",  skipHeader=true)
  val offers = Csv(offersFile, separator = ",", skipHeader = true)

  // debug
  val dbg = PipeDebug().toStdOut.printFieldsEvery(Some(5))

  // repeater counts for offers
  val repeaters =
    train
      .filter('repeater ) { repeater : String => repeater == "t" }
      .groupBy('offer) { _.size }
      .rename('size -> 'numRepeaters)

  //  transactions with training and offer data
  val trainOffersTransaction =
    transactions
      .discard('category, 'brand, 'company, 'date)
        .joinWithSmaller(('id,'chain) -> ('id,'chain), train) // train*
        .joinWithTiny('offer -> 'offer, offers) // offers
        .joinWithSmaller('offer -> 'offer, repeaters)

  // feature schemas
  val productSizeFeatures = ('productSizeCount, 'productSizeMean, 'productSizeStdDev)
  val purchaseQuantityFeatures = ('purchaseQuantityCount, 'purchaseQuantityMean, 'purchaseQuantityStdDev)
  val purchaseAmountFeatures = ('purchaseAmountCount, 'purchaseAmountMean,'purchaseAmountStdDev)
  val repeaterFeatures = ('repeaterCount, 'repeaterMean, 'repeaterStdDev)

  // labelled data
  val trainingData =
    trainOffersTransaction
      .groupBy('id, 'offer, 'category, 'brand, 'company, 'chain, 'dept, 'productmeasure) {
        _.sizeAveStdev('productsize -> productSizeFeatures)
         .sizeAveStdev('purchasequantity -> purchaseQuantityFeatures)
         .sizeAveStdev('purchaseamount -> purchaseAmountFeatures)
       //.sizeAveStdev('numRepeaters -> repeaterFeatures)
      }
      .joinWithSmaller(('id, 'chain, 'offer) -> ('id, 'chain, 'offer), train)
      .discard('repeattrips, 'offerdate)
      .debug(dbg)
      .write(NullSource)
}
