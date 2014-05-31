package org.cdl.scalding

import com.twitter.scalding._


class Features(args : Args) extends Job(args) {

  def dirs = Map("d" -> "-debug.csv", "p" -> "-prod.csv")

  // Inputs
  val trainHistoryFile = args("trainHistory")
  val testHistoryFile = args("testHistory")
  val transactionsFile = args("transactions")
  val offersFile = args("offers")

  // Debug outputs

  //id,chain,offer,market,repeattrips,repeater,offerdate
  val train = Csv(trainHistoryFile, separator = ",", skipHeader = true)
  val test = Csv(testHistoryFile, separator = ",",  skipHeader=true)
  val transactions = Csv(transactionsFile, separator = ",",  skipHeader=true)
  val offers = Csv(offersFile, separator = ",", skipHeader = true)

  val dbg = PipeDebug().toStdOut.printFieldsEvery(Some(5))

  val repeaters =
    train
      .filter('repeater ) { repeater : String => repeater == "t" }
      .groupBy('offer) { _.size }
      .rename('size -> 'numRepeaters)

  //  transactions with training data
  val trainOffersTransaction =
    transactions
      .discard('category, 'brand, 'company, 'date)
        .joinWithSmaller(('id,'chain) -> ('id,'chain), train) // train*
        .joinWithTiny('offer -> 'offer, offers) // offers
        .joinWithSmaller('offer -> 'offer, repeaters)
//      .debug(dbg)
//      .write(NullSource)


//id,chain,dept,category,company,brand,date,productsize,productmeasure,purchasequantity,purchaseamount

  // Features schemas
  val productSizeFeatures = ('productSizeCount, 'productSizeMean, 'productSizeStdDev)
  val purchaseQuantityFeatures = ('purchaseQuantityCount, 'purchaseQuantityMean, 'purchaseQuantityStdDev)
  val purchaseAmountFeatures = ('purchaseAmountCount, 'purchaseAmountMean,'purchaseAmountStdDev)
  val repeaterFeatures = ('repeaterCount, 'repeaterMean, 'repeaterStdDev)

  val offerFeatures =
    trainOffersTransaction
      .groupBy('id, 'offer, 'category, 'brand, 'company, 'chain, 'dept, 'productmeasure) {
        _.sizeAveStdev('productsize -> productSizeFeatures)
          .sizeAveStdev('purchasequantity -> purchaseQuantityFeatures)
          .sizeAveStdev('purchaseamount -> purchaseAmountFeatures)
//          .sizeAveStdev('numRepeaters -> repeaterFeatures)
      }
      .joinWithSmaller(('id, 'chain, 'offer) -> ('id, 'chain, 'offer), train)
      .discard('repeattrips, 'offerdate)
      .debug(dbg)
      .write(NullSource)

}
