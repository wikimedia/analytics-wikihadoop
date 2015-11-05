package org.wikimedia.wikihadoop.job

import _root_.scopt.OptionParser
import org.apache.crunch.io.Compress
import org.apache.crunch.scrunch.PipelineApp
import org.apache.crunch.types.writable.Writables
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.wikimedia.wikihadoop.inputformatnewapi.MediaWikiRevisionXMLToFlatJSONInputFormat

/**
 * Created by Joseph Allemandou
 * Date: 7/15/15
 * Time: 9:19 AM
 * Copyright Joseph Allemandou - 2014
 */
object JsonRevisionsSortedPerPageSimple extends PipelineApp with Logging {

  var params: Params = Params()

  /**
   * Config class for CLI argument parser using scopt
   */
  case class Params(inputPath: String = "",
                    outputPath: String = "",
                    numReducers: Int = 0,
                    taskTimeout: Int = 1800000,
                    mapperMb: Int = 4096,
                    mapperMbHeap: Int = 3584,
                    reducerMb: Int = 6144,
                    reducerMbHeap: Int = 5632)

  /**
   * Define the command line options parser
   */
  val argsParser = new OptionParser[Params]("JSON revisions sorted per Page from XML wikidumps") {
    head("JSON revisions sorted per Page from XML wikidumps", "")
    note(
      """This job extracts JSON revisions from wikidumps,
        | grouping them per page and sort them by timestamp.""".format().stripMargin)
    help("help") text "Prints this usage text"

    opt[String]('i', "input-path") required() valueName ("<path>") action { (x, p) =>
      p.copy(inputPath = x)
    } text "Input path for the XML wikidump file(s)."

    opt[String]('o', "output-path") required() valueName ("<path>") action { (x, p) =>
      p.copy(outputPath = if (x.endsWith("/")) x else x + "/")
    } text "Output path where to store the results."

    opt[Int]('r', "reducers") required() action { (x, p) =>
      p.copy(numReducers = x)
    } text "Number of reducers to use."

    opt[Int]("mapper-mb") optional() action { (x, p) =>
      p.copy(mapperMb = x)
    } text "Yarn memory to allocate per mapper in Mb (default to 4096)."

    opt[Int]("mapper-mb-heap") optional() action { (x, p) =>
      p.copy(mapperMbHeap = x)
    } text "JVM heap to allocate per mapper in Mb (should be less than mapper-mb, default to 3584)."

    opt[Int]("reducer-mb") optional() action { (x, p) =>
      p.copy(reducerMb = x)
    } text "Yarn memory to allocate per reducer in Mb (default to 6144)."

    opt[Int]("reducer-mb-heap") optional() action { (x, p) =>
      p.copy(reducerMbHeap = x)
    } text "JVM heap to allocate per reducer in Mb (should be less than reducer-mb, default to 5632)."

    opt[Int]("task-timeout") optional() action { (x, p) =>
      p.copy(taskTimeout = x)
    } text "Number of milliseconds before task timeout (default to 1800000 = 1/2h)."

  }

  override def run(args: Array[String]) {

    argsParser.parse(args, Params()) match {
      case Some(params) => {

        this.params = params

        this.configuration.set("mapreduce.map.java.opts", "-Xmx%dm".format(params.mapperMbHeap))
        this.configuration.set("mapreduce.map.memory.mb", "%d".format(params.mapperMb))
        this.configuration.set("mapreduce.reduce.java.opts", "-Xmx%dm".format(params.reducerMbHeap))
        this.configuration.set("mapreduce.reduce.memory.mb", "%d".format(params.reducerMb))
        this.configuration.set("mapred.task.timeout", "%d".format(params.taskTimeout))

        val revJsonSource = from.formattedFile(
          params.inputPath,
          classOf[MediaWikiRevisionXMLToFlatJSONInputFormat],
          Writables.longs,
          Writables.strings
        )

        val sortedRevs = read(revJsonSource).values()
          .map(jsonString => {
            val rev = parse(jsonString)
            ((rev \ "page_id").asInstanceOf[JInt].num.toLong,
              ((rev \ "timestamp").asInstanceOf[JString].s,
                jsonString))
          })
          .secondarySortAndMap((_: Long, itv: Iterable[(String, String)]) => itv.map(_._2),
            numReducers = params.numReducers)

        val textFileTarget =  Compress.compress(to.textFile(params.outputPath),
          classOf[org.apache.hadoop.io.compress.BZip2Codec])
        write(sortedRevs, textFileTarget)

      }
      case None => sys.exit(1)
    }

  }

}
