package org.wikimedia.wikihadoop.job

import _root_.scopt.OptionParser
import org.apache.crunch.io.Compress
import org.apache.crunch.scrunch.{PTable, PipelineApp}
import org.apache.crunch.types.writable.Writables
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.wikimedia.wikihadoop.inputformatnewapi.{MediaWikiRevisionXMLToFlatJSONInputFormat, MediaWikiRevisionXMLToJSONInputFormat}

/**
 * Created by Joseph Allemandou
 * Date: 7/15/15
 * Time: 9:19 AM
 * Copyright Joseph Allemandou - 2014
 */
object JsonRevisionsSortedPerPageScrunch extends PipelineApp with Logging {

  var params: Params = Params()

  /**
   * Config class for CLI argument parser using scopt
   */
  case class Params(inputPath: String = "",
                    outputPath: String = "",
                    sort: Boolean = true,
                    nsZeroOnly: Boolean = false,
                    metadataOnly: Boolean = false,
                    flattenedJson: Boolean = true,
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

    opt[Unit]('n', "not-sorted") action { (_, p) =>
      p.copy(sort = true)
    } text "Sort revisions or leave."

    opt[Unit]('z', "ns-zero-only") action { (_, p) =>
      p.copy(nsZeroOnly = true)
    } text "Filter to keep only namespace 0 pages."

    opt[Unit]('m', "metadata-only") action { (_, p) =>
      p.copy(metadataOnly = true)
    } text "Filter to keep only metadata information (no text)."

    opt[Unit]('h', "hierarchical-json") action { (_, p) =>
      p.copy(flattenedJson = false)
    } text "Generate hierarchical json instead of flattened one."

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

  def flatMapFun(jsonString: String): TraversableOnce[(Long, (String, String))] = {
    val rev = parse(jsonString)
    // Remove full raw if namespace zero only and namespace != 0
    if (params.nsZeroOnly && ((params.flattenedJson && (rev \ "page_namespace").asInstanceOf[JInt].num.toLong == 0L)
        || (!params.flattenedJson && (rev \ "page" \ "namespace").asInstanceOf[JInt].num.toLong == 0L)))
      Seq.empty[(Long,(String, String))]
    else {
      // Remove text if metadata only
      if (params.metadataOnly) {
        val frev = rev removeField {
          case JField("text", _) => true
          case _ => false
        }
        Seq((if (params.flattenedJson) (frev \ "page_id").asInstanceOf[JInt].num.toLong else (frev \ "page" \ "id").asInstanceOf[JInt].num.toLong,
          ((frev \ "timestamp").asInstanceOf[JString].s,
            frev toString)))
      } else {
        Seq((if (params.flattenedJson) (rev \ "page_id").asInstanceOf[JInt].num.toLong else (rev \ "page" \ "id").asInstanceOf[JInt].num.toLong,
          ((rev \ "timestamp").asInstanceOf[JString].s,
            jsonString)))
      }
    }
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
          if (params.flattenedJson) classOf[MediaWikiRevisionXMLToFlatJSONInputFormat] else classOf[MediaWikiRevisionXMLToJSONInputFormat],
          Writables.longs,
          Writables.strings
        )

        val revsToSort: PTable[Long, (String, String)] = read(revJsonSource).values()
          .flatMap(flatMapFun)

        val sortedRevs = revsToSort.secondarySortAndFlatMap(
          (_: Long, itv: Iterable[(String, String)]) => for (v <- itv) yield v._2,
          numReducers = params.numReducers)

        val textFileTarget =  Compress.compress(to.textFile(params.outputPath),
          classOf[org.apache.hadoop.io.compress.BZip2Codec])
        write(sortedRevs, textFileTarget)

      }
      case None => sys.exit(1)
    }

  }

}
