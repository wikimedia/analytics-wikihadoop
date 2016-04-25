package org.wikimedia.wikihadoop.job

import org.wikimedia.wikihadoop.job.JsonRevisionsSortedPerPageMapReduce._

import scala.collection.JavaConversions._
import java.io.{DataInput, DataOutput}
import java.lang.Iterable

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.wikimedia.wikihadoop.inputformatnewapi.MediaWikiRevisionXMLToFlatJSONInputFormat
import scopt.OptionParser


/**
 * Created by jo on 11/18/15.
 */
class JsonRevisionsSortedPerPageMapReduce extends Configured with Tool with Logging {

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
                    taskTimeout: Long = 1800000,
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

    /*opt[Unit]('n', "not-sorted") action { (_, p) =>
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
    } text "Generate hierarchical json instead of flattened one."*/

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

    opt[Long]("task-timeout") optional() action { (x, p) =>
      p.copy(taskTimeout = x)
    } text "Number of milliseconds before task timeout (default to 1800000 = 1/2h)."

  }

  override def run(args: Array[String]): Int = {
    argsParser.parse(args, Params()) match {
      case Some(params) => {

        this.params = params

        // Configuration from Tool
        val conf: Configuration = getConf

        // Set configuration from params
        conf.set("mapreduce.job.reduces", "%d".format(params.numReducers))
        conf.set("mapreduce.map.java.opts", "-Xmx%dm".format(params.mapperMbHeap))
        conf.set("mapreduce.map.memory.mb", "%d".format(params.mapperMb))
        conf.set("mapreduce.reduce.java.opts", "-Xmx%dm".format(params.reducerMbHeap))
        conf.set("mapreduce.reduce.memory.mb", "%d".format(params.reducerMb))
        conf.set("mapred.task.timeout", "%d".format(params.taskTimeout))

        //Set BZ2 compression
        conf.set("mapreduce.output.fileoutputformat.compress", "true")
        conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.BZip2Codec")


        // job configuration and launch
        val job: Job = Job.getInstance(conf, "JsonRevisionsSortedPerPageMapReduce")
        job.setJarByClass(classOf[JsonRevisionsSortedPerPageMapReduce])

        // Input
        job.setInputFormatClass(classOf[MediaWikiRevisionXMLToFlatJSONInputFormat])
        FileInputFormat.addInputPath(job, new Path(params.inputPath))

        // Map
        job.setMapperClass(classOf[JsonRevisionSortedPerPageMapper])
        job.setMapOutputKeyClass(classOf[PageIdRevDatePair])
        job.setMapOutputValueClass(classOf[Text])

        // Sort
        job.setPartitionerClass(classOf[PageIdRevDatePairPartitioner])
        job.setGroupingComparatorClass(classOf[PageIdRevDatePairGroupingComparator])
        job.setSortComparatorClass(classOf[PageIdRevDatePairSortComparator])

        // Reduce
        job.setOutputKeyClass(classOf[NullWritable])
        job.setOutputValueClass(classOf[Text])
        job.setReducerClass(classOf[JsonRevisionSortedPerPageReducer]);

        // Output
        job.setOutputFormatClass(classOf[TextOutputFormat[NullWritable, Text]])
        FileOutputFormat.setOutputPath(job, new Path(params.outputPath));

        // Launch
        return (if (job.waitForCompletion(true)) 0 else 1)

      }
      case None => return 1
    }

  }
}

object JsonRevisionsSortedPerPageMapReduce {
  /**
   * Comparable Pair class for secondary sort
   */
  class PageIdRevDatePair extends Writable with WritableComparable[PageIdRevDatePair] {
    val pageId: LongWritable = new LongWritable
    val revDate: Text = new Text
    def PageIdRevDatePair() {}
    override def write(dataOutput: DataOutput): Unit = {
      this.pageId.write(dataOutput)
      this.revDate.write(dataOutput)
    }
    override def readFields(dataInput: DataInput): Unit = {
      this.pageId.readFields(dataInput)
      this.revDate.readFields(dataInput)
    }
    override def compareTo(other: PageIdRevDatePair): Int = {
      val comparePageId = this.pageId.compareTo(other.pageId)
      return if (comparePageId == 0) this.revDate.compareTo(other.revDate) else comparePageId
    }

    def getPageId(): Long = this.pageId.get
    def setPageId(pageId: Long): Unit = this.pageId.set(pageId)

    def getRevDate(): String = this.revDate.toString
    def setRevDate(revDate: String): Unit = this.revDate.set(revDate)

  }

  /**
   * Mapper - parse json to extract pageId and revDate
   */
  class JsonRevisionSortedPerPageMapper extends Mapper[LongWritable, Text, PageIdRevDatePair, Text] with Logging {
    val pageIdRevDatePair: PageIdRevDatePair = new PageIdRevDatePair
    val outputText: Text = new Text
    def JsonRevisionSortedPerPageMapper() = {}
    override def setup(context: Mapper[LongWritable, Text, PageIdRevDatePair, Text]#Context): Unit = {
      super.setup(context)
    }
    override def map(key: LongWritable, value: Text,
                     context: Mapper[LongWritable, Text, PageIdRevDatePair, Text]#Context): Unit = {
      val jsonRev = parse(value.toString)
      this.pageIdRevDatePair.setPageId((jsonRev \ "page_id").asInstanceOf[JInt].num.toLong)
      this.pageIdRevDatePair.setRevDate((jsonRev \ "timestamp").asInstanceOf[JString].s)
      context.write(this.pageIdRevDatePair, value)
    }
  }

  /**
   * Partitioner - Uses pageId
   */
  class PageIdRevDatePairPartitioner extends Partitioner[PageIdRevDatePair, Text] with Logging {
    override def getPartition(key: PageIdRevDatePair, value: Text, numPartitions: Int): Int = {
      return key.getPageId().hashCode() % numPartitions
    }
  }

  /**
   * Grouping Comparator - Uses PageId
   */
  class PageIdRevDatePairGroupingComparator extends WritableComparator(classOf[PageIdRevDatePair], true) {
    override def compare(p1: WritableComparable[_], p2: WritableComparable[_]): Int = {
      return p1.asInstanceOf[PageIdRevDatePair].getPageId().compareTo(p2.asInstanceOf[PageIdRevDatePair].getPageId())
    }
  }

  /**
   * Sort Comparator - Uses PageId and RevDate
   */
  class PageIdRevDatePairSortComparator extends WritableComparator(classOf[PageIdRevDatePair], true) {
    override def compare(p1: WritableComparable[_], p2: WritableComparable[_]): Int = {
      return p1.asInstanceOf[PageIdRevDatePair].compareTo(p2.asInstanceOf[PageIdRevDatePair])
    }
  }

  /**
   * Reducer - Output text
   */
  class JsonRevisionSortedPerPageReducer extends Reducer[PageIdRevDatePair, Text, NullWritable, Text] with Logging {
    val nullKey = NullWritable.get()
    def JsonRevisionSortedPerPageReducer() = {}
    override def setup(context: Reducer[PageIdRevDatePair, Text, NullWritable, Text]#Context): Unit = {
      super.setup(context)
    }
    override def reduce(key: PageIdRevDatePair, values: Iterable[Text],
                        context: Reducer[PageIdRevDatePair, Text, NullWritable, Text]#Context): Unit = {
      for (v <- values.iterator()) context.write(nullKey, v)
    }
  }

  def main(args: Array[String]): Unit = {
    System.exit(ToolRunner.run(new Configuration, new JsonRevisionsSortedPerPageMapReduce, args))
  }
}
