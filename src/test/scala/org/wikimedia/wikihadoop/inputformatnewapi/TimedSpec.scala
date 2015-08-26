package org.wikimedia.wikihadoop.inputformatnewapi

import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by jo on 4/7/15.
 */
class TimedSpec extends FlatSpec with Matchers {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

}
