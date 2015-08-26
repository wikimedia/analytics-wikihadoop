package org.wikimedia.wikihadoop.job

import _root_.scopt.OptionParser
import org.apache.crunch.io.Compress
import org.apache.crunch.scrunch.PipelineApp
import org.apache.crunch.types.writable.Writables
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.wikimedia.wikihadoop.inputformatnewapi.MediaWikiRevisionXMLToJSONInputFormat

/**
 * Created by Joseph Allemandou
 * Date: 7/15/15
 * Time: 9:19 AM
 * Copyright Joseph Allemandou - 2014
 */
object JsonRevisionsSortedPerPage extends PipelineApp with Logging {

  /**
   * Config class for CLI argument parser using scopt
   */
  case class Params(inputPath: String = "",
                    outputPath: String = "",
                    numReducers: Int = 0,
                    nsZeroOnly: Boolean = false,
                    taskTimeout: Int = 1800000,
                    mapperMb: Int = 4096,
                    reducerMb: Int = 6144 )

  /**
   * Define the command line options parser
   */
  val argsParser = new OptionParser[Params]("JSON revisions sorted per Page from XML wikidumps") {
    head("JSON revisions sorted per Page from XML wikidumps", "")
    note(
      """This job extracts JSON revisions from wikidumps,
        | grouping them per page and sort them by timestamp.""".format().stripMargin)
    help("help") text ("Prints this usage text")

    opt[String]('i', "input-path") required() valueName ("<path>") action { (x, p) =>
      p.copy(inputPath = x)
    } text ("Input path for the XML wikidump file(s).")

    opt[String]('o', "output-path") required() valueName ("<path>") action { (x, p) =>
      p.copy(outputPath = if (x.endsWith("/")) x else x + "/")
    } text ("Output path where to store the results.")

    opt[Int]('r', "reducers") required() action { (x, p) =>
      p.copy(numReducers = x)
    } text ("Number of reducers to use.")

    opt[Unit]("ns-zero-only") action { (_, p) =>
      p.copy(nsZeroOnly = true)
    } text ("Filter to keep only namespace 0 pages.")

    opt[Int]("mapper-mb") optional() action { (x, p) =>
      p.copy(mapperMb = x)
    } text ("Memory to allocate per mapper in Mb (default to 4096).")

    opt[Int]("reducer-mb") optional() action { (x, p) =>
      p.copy(reducerMb = x)
    } text ("Memory to allocate per reducer in Mb (default to 6144).")

    opt[Int]("task-tiemout") optional() action { (x, p) =>
      p.copy(taskTimeout = x)
    } text ("Number of milliseconds before task timeout (default to 1800000 = 1/2h).")

  }

  override def run(args: Array[String]) {

    argsParser.parse(args, Params()) match {
      case Some(params) => {

        this.configuration.set("mapreduce.map.java.opts", "-Xmx%dm".format(params.mapperMb))
        this.configuration.set("mapreduce.map.memory.mb", "%d".format(params.mapperMb))
        this.configuration.set("mapreduce.reduce.java.opts", "-Xmx%dm".format(params.reducerMb))
        this.configuration.set("mapreduce.reduce.memory.mb", "%d".format(params.reducerMb))
        this.configuration.set("mapred.task.timeout", "%d".format(params.taskTimeout))

        val revJsonSource = from.formattedFile(
          params.inputPath,
          classOf[MediaWikiRevisionXMLToJSONInputFormat],
          Writables.longs,
          Writables.strings
        )

        val revs = read(revJsonSource).values()
          .map(jsonString => {
            val rev = parse(jsonString)
            ((rev \ "page" \ "namespace").asInstanceOf[JInt].num.toLong,
              ((rev \ "page" \ "id").asInstanceOf[JInt].num.toLong,
                ((rev \ "timestamp").asInstanceOf[JString].s, jsonString)))
          }).asPCollection()

        val filteredRevs = if (params.nsZeroOnly) revs.filter(_._1 == 0L) else revs

        val revsToSort = filteredRevs.map(_._2)

        val sortedRevs = revsToSort.secondarySortAndFlatMap(
          (_, itv: Iterable[(String, String)]) => itv.map(_._2),
          numReducers = params.numReducers)

        val textFileTarget =  Compress.compress(to.textFile(params.outputPath),
          classOf[org.apache.hadoop.io.compress.BZip2Codec])
        write(sortedRevs, textFileTarget)

      }
      case None => sys.exit(1)
    }

  }

}
