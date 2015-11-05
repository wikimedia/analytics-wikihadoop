package org.wikimedia.wikihadoop.inputformatnewapi

import java.io.ByteArrayInputStream

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.{CompressionCodecFactory, SplittableCompressionCodec}
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

/**
 * Modified from DBPedia distibuted extraction framework (https://github.com/dbpedia/distributed-extraction-framework)
 *
 * Hadoop InputFormat that splits a Wikipedia dump file into json revision strings
 * adding current page metadata as fields in the revision
 *
 * The WikiRevisionXMLToJSONRecordReader class inside outputs a Text as value and the starting position (byte) as key.
 *
 */
abstract class MediaWikiRevisionXMLTransformerInputFormat[TOF <: MediaWikiObjectsFactory](objectsFactory: TOF) extends FileInputFormat[LongWritable, Text] {

  private val LOG = LogFactory.getLog(classOf[MediaWikiRevisionXMLTransformerInputFormat[TOF]])

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

    new WikiRevisionXMLTransformerRecordReader[TOF](split, context, objectsFactory)
  }

  private class WikiRevisionXMLTransformerRecordReader[TOF <: MediaWikiObjectsFactory](split: FileSplit, context: TaskAttemptContext, objectsFactory: TOF) extends RecordReader[LongWritable, Text] {
    private var key: LongWritable = null
    private var value: Text = null

    private val conf = context.getConfiguration

    private val contentBuffer = new DataOutputBuffer()

    private var isFirstAfterInit = false

    private val inputStream = SeekableInputStream(split,
      split.getPath.getFileSystem(conf),
      new CompressionCodecFactory(conf))

    private val matcher = new ByteMatcher(inputStream)

    private var byteArrayInputStream: ByteArrayInputStream = null

    private val mediaWikiXMLParser = new MediaWikiXMLParser(objectsFactory)
    private var pageMetaData: mediaWikiXMLParser.objectsFactory.V = null.asInstanceOf[mediaWikiXMLParser.objectsFactory.V]

    private val (start, end) = {
      inputStream match {
        case SeekableSplitCompressedInputStream(sin) =>
          (sin.getAdjustedStart, sin.getAdjustedEnd + 1)
        case _ =>
          (split.getStart, split.getStart + split.getLength)
      }
    }

    private val revisionBeginPattern = "<revision>".getBytes("UTF-8")
    private val revisionEndPattern = "</revision>".getBytes("UTF-8")
    private val pageBeginPattern = "<page>".getBytes("UTF-8")
    private val pageEndPattern = "</page>".getBytes("UTF-8")

    override def close() = inputStream.close()

    override def getProgress: Float = {
      if (end == start) 1.0f else (getPos - start).asInstanceOf[Float] / (end - start).asInstanceOf[Float]
    }

    def getPos: Long = matcher.getPos

    override def initialize(genericInputSplit: InputSplit, context: TaskAttemptContext) = {
      contentBuffer.reset()
      // Loop until page context initialized - Restrict research in current split
      while (pageMetaData == null && matcher.getPos < end) {
        // Look for page beginning
        if (matcher.getPos < end && matcher.readUntilMatch(pageBeginPattern, end)) {
          // Initialize contentBuffer with page beginning tag (removed by parsing)
          contentBuffer.write(pageBeginPattern)
          // Read until next revision beginning (could be in next split)
          // keeping page metadata in between <page> and <revision> tags.
          if (matcher.readUntilMatch(revisionBeginPattern, Some(contentBuffer))) {
            // Get buffer data removing revision tag
            val contentStr = contentBuffer.getData.take(contentBuffer.getLength - revisionBeginPattern.length)
            // Convert metadata xml (adding page end tag) to a map
            byteArrayInputStream = new ByteArrayInputStream(contentStr ++ pageEndPattern)
            pageMetaData = mediaWikiXMLParser.parsePageMetaData(mediaWikiXMLParser.initializeXmlStreamReader(byteArrayInputStream))
            // Clean buffer
            contentBuffer.reset()
            isFirstAfterInit = true
          }
        }
      }
    }

    override def nextKeyValue(): Boolean = {
      // If pageMetaData not initialized, no page in this split - return false
      if (pageMetaData == null)
        return false

      // Initialize key and value
      if (key == null) key = new LongWritable()
      if (value == null) value = new Text()

      // Check if we already are after the end of the split to prevent starting
      // reading new pages, but finish the current one
      val afterEnd = matcher.getPos >= end

      // Read until next revision beginning (could be in next split)
      // keeping content between previous revision and next to possibly update page data
      if (! isFirstAfterInit && matcher.readUntilMatch(revisionBeginPattern, Some(contentBuffer))) {
        // Get content in between revisions
        val contentStr = contentBuffer.getData.take(contentBuffer.getLength)
        // Look for page begin tag in data in between revisions
        val pagePos = contentStr.indexOfSlice(pageBeginPattern)
        // If we are at new page
        if (pagePos >= 0) {
          // If we already are after the end of our split, stop reading new pages
          if (afterEnd)
            return false
          // Else update pageMetaData context
          // Convert metadata xml to a map
          // getting buffer data starting from pagePos, removing revision begin tag and adding page end tag
          byteArrayInputStream = new ByteArrayInputStream(contentStr.slice(pagePos, contentStr.length - revisionBeginPattern.length) ++ pageEndPattern)
          pageMetaData = mediaWikiXMLParser.parsePageMetaData(mediaWikiXMLParser.initializeXmlStreamReader(byteArrayInputStream))

        }
        // Clean buffer
        contentBuffer.reset()
      }

      // Initialize contentBuffer with revision tag (removed by parsing)
      contentBuffer.write(revisionBeginPattern)
      isFirstAfterInit = false

      // Read until end of revision (could be in next split)
      // keeping revision content
      if (matcher.readUntilMatch(revisionEndPattern, Some(contentBuffer))) {

        // Key is set to the position (bytes) of the revision end
        key.set(matcher.getPos)

        // Json Object of revision is created parsing the XML
        // and adding context page meta data
        val byteArrayInputStream = new ByteArrayInputStream(contentBuffer.getData.take(contentBuffer.getLength))
        value.set(mediaWikiXMLParser.objectsFactory.toText(mediaWikiXMLParser.parseRevision(
          mediaWikiXMLParser.initializeXmlStreamReader(byteArrayInputStream), pageMetaData)))
        contentBuffer.reset()

        return true
      }
      contentBuffer.reset()
      false
    }

    override def getCurrentKey: LongWritable = key

    override def getCurrentValue: Text = value

  }
}
