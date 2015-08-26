package org.wikimedia.wikihadoop.job

import org.apache.log4j.Logger

/**
 * Created by Joseph Allemandou
 * Date: 7/16/15
 * Time: 8:48 AM
 * Copyright Joseph Allemandou - 2014
 */
trait Logging {

  val logger : Logger = Logger.getLogger(this.getClass)

}
