package org.wikimedia.wikihadoop.inputformatnewapi

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.util.ReflectionUtils

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Created by jo on 3/9/15.
 */
class MediaWikiPageXMLInputFormatSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  "MediaWikiPageXMLInputFormat" should " read XML dump file in pages " in {

    val hadoopConf :Configuration = new Configuration(false)
    hadoopConf.set("fs.default.name", "file:///")

    val testFilePath = getClass.getResource("/mediawiki_dump_test.xml").getFile
    val path = new Path(testFilePath)
    val split = new FileSplit(path, 0, new File(testFilePath).length(), null)

    val inputFormat = ReflectionUtils.newInstance(classOf[MediaWikiPageXMLInputFormat], hadoopConf)
    val context = new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID())
    val reader= inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)

    var pageCount = 0
    val expectedPages = List(
      "<page>\n        <id>1</id>\n        <ns>1</ns>\n        <title>Page title</title>\n        <restrictions>edit=sysop:move=sysop</restrictions>\n        <redirect title=\"Redirect title\" />\n        <revision>\n            <timestamp>2001-01-15T13:15:00Z</timestamp>\n            <contributor><username>Foobar</username></contributor>\n            <comment>I have just one thing to say!</comment>\n            <text>\n                A bunch of [[text]] here.\n                With new lines for instance.\n            </text>\n            <minor />\n        </revision>\n        <revision>\n            <timestamp>2001-01-15T13:10:27Z</timestamp>\n            <contributor><ip>10.0.0.2</ip></contributor>\n            <comment>new!</comment>\n            <text>An earlier [[revision]].</text>\n        </revision>\n    </page>",
      "<page>\n        <title>Talk:Page title</title>\n        <revision>\n            <timestamp>2001-01-15T14:03:00Z</timestamp>\n            <contributor><ip>10.0.0.2</ip></contributor>\n            <comment>hey</comment>\n            <text>WHYD YOU LOCK PAGE??!!! i was editing that jerk</text>\n        </revision>\n    </page>"
    )
    while (reader.nextKeyValue()) {
      val key = reader.getCurrentKey
      val value = reader.getCurrentValue
      key should not be null
      value should not be null
      value.toString shouldEqual expectedPages(pageCount)
      pageCount += 1
    }
  }

}