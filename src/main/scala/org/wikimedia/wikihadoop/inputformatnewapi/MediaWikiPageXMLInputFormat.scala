package org.wikimedia.wikihadoop.inputformatnewapi

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

/**
 * Modified from DBPedia distibuted extraction framework (https://github.com/dbpedia/distributed-extraction-framework)
 *
 * Hadoop InputFormat that splits a Wikipedia dump file into page chunks.
 *
 * The WikiPageXMLRecordReader class inside outputs a Text as value and the starting position (byte) as key.
 */
class MediaWikiPageXMLInputFormat extends FileInputFormat[LongWritable, Text] {

  private val LOG = LogFactory.getLog(classOf[MediaWikiPageXMLInputFormat])

  protected override def isSplitable(context: JobContext, file: Path): Boolean =
  {
    val codec = new CompressionCodecFactory(context.getConfiguration).getCodec(file)
    if (null == codec) true else codec.isInstanceOf[SplittableCompressionCodec]
  }

  override def createRecordReader(genericSplit: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] =
  {
    val split = genericSplit.asInstanceOf[FileSplit]
    LOG.info("getRecordReader start.....split=" + split)
    context.setStatus(split.toString)
    new WikiPageXMLRecordReader(split, context)
  }

  private class WikiPageXMLRecordReader(split: FileSplit, context: TaskAttemptContext) extends RecordReader[LongWritable, Text]
  {
    private var key: LongWritable = null
    private var value: Text = null

    private val conf = context.getConfiguration

    private val page = new DataOutputBuffer()
    private val inputStream = SeekableInputStream(split,
      split.getPath.getFileSystem(conf),
      new CompressionCodecFactory(conf))
    private val matcher = new ByteMatcher(inputStream)

    private val (start, end) =
    {
      inputStream match
      {
        case SeekableSplitCompressedInputStream(sin) =>
          (sin.getAdjustedStart, sin.getAdjustedEnd + 1)
        case _ =>
          (split.getStart, split.getStart + split.getLength)
      }
    }

    private val pageBeginPattern = "<page>".getBytes("UTF-8")
    private val pageEndPattern = "</page>".getBytes("UTF-8")

    override def close() = inputStream.close()

    override def getProgress: Float =
    {
      if (end == start) 1.0f else (getPos - start).asInstanceOf[Float] / (end - start).asInstanceOf[Float]
    }

    def getPos: Long = matcher.getPos

    override def initialize(genericInputSplit: InputSplit, context: TaskAttemptContext) = ()

    override def nextKeyValue(): Boolean =
    {
      // Initialize key and value
      if (key == null) key = new LongWritable()
      if (value == null) value = new Text()

      if (matcher.getPos < end && matcher.readUntilMatch(pageBeginPattern, end))
      {
        try
        {
          page.write(pageBeginPattern)
          if (matcher.readUntilMatch(pageEndPattern, end, Some(page)))
          {
            // Key is set to the position (bytes) where the page is found
            key.set(matcher.getPos)

            value.set(page.getData.take(page.getLength))

            return true
          }
        }
        finally
        {
          page.reset()
        }
      }
      false
    }

    override def getCurrentKey: LongWritable = key

    override def getCurrentValue: Text = value
  }
}
