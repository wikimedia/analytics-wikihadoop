package org.wikimedia.wikihadoop.inputformatnewapi

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptID
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.hadoop.util.ReflectionUtils
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Created by jo on 3/9/15.
 */
class MediaWikiRevisionXMLToJSONInputFormatSpec extends FlatSpec with Matchers with BeforeAndAfterAll {

  "MediaWikiRevisionXMLToJSONInputFormat" should " read XML dump file in json revisions " in {

    val hadoopConf :Configuration = new Configuration(false)
    hadoopConf.set("fs.default.name", "file:///")

    val testFilePath = getClass.getResource("/mediawiki_dump_test.xml").getFile
    val path = new Path(testFilePath)
    val split = new FileSplit(path, 0, new File(testFilePath).length(), null)

    val inputFormat = ReflectionUtils.newInstance(classOf[MediaWikiRevisionXMLToJSONInputFormat], hadoopConf)
    val context = new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID())
    val reader= inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)

    var revisionsCount = 0
    val expectedRevisions = List(
      "{\"sha1\":\"\",\"format\":\"\",\"contributor\":{\"id\":-1,\"user_text\":\"Foobar\"},\"timestamp\":\"2001-01-15T13:15:00Z\",\"model\":\"\",\"parent_id\":-1,\"page\":{\"redirect_title\":\"Redirect title\",\"restrictions\":[\"edit=sysop:move=sysop\"],\"id\":1,\"title\":\"Page title\",\"namespace\":1},\"text\":\"\\n                A bunch of [[text]] here.\\n                With new lines for instance.\\n            \",\"bytes\":100,\"id\":-1,\"comment\":\"I have just one thing to say!\",\"minor\":true}",
      "{\"sha1\":\"\",\"format\":\"\",\"contributor\":{\"id\":-1,\"user_text\":\"10.0.0.2\"},\"timestamp\":\"2001-01-15T13:10:27Z\",\"model\":\"\",\"parent_id\":-1,\"page\":{\"redirect_title\":\"Redirect title\",\"restrictions\":[\"edit=sysop:move=sysop\"],\"id\":1,\"title\":\"Page title\",\"namespace\":1},\"text\":\"An earlier [[revision]].\",\"bytes\":24,\"id\":-1,\"comment\":\"new!\",\"minor\":false}",
      "{\"sha1\":\"\",\"format\":\"\",\"contributor\":{\"id\":-1,\"user_text\":\"10.0.0.2\"},\"timestamp\":\"2001-01-15T14:03:00Z\",\"model\":\"\",\"parent_id\":-1,\"page\":{\"redirect_title\":\"\",\"restrictions\":[],\"id\":-1,\"title\":\"Talk:Page title\",\"namespace\":-1},\"text\":\"WHYD YOU LOCK PAGE??!!! i was editing that jerk\",\"bytes\":47,\"id\":-1,\"comment\":\"hey\",\"minor\":false}"
    )
    while (reader.nextKeyValue()) {
      val key = reader.getCurrentKey
      val value = reader.getCurrentValue
      key should not be null
      value should not be null
      value.toString shouldEqual expectedRevisions(revisionsCount)
      revisionsCount += 1
    }
  }

  "MediaWikiRevisionXMLToJSONInputFormat" should "not return any result for file not containing page" in {

    val hadoopConf :Configuration = new Configuration(false)
    hadoopConf.set("fs.default.name", "file:///")

    val testFilePath = getClass.getResource("/dummy_test_data.txt").getFile
    val path = new Path(testFilePath)
    val split = new FileSplit(path, 0, new File(testFilePath).length(), null)

    val inputFormat = ReflectionUtils.newInstance(classOf[MediaWikiRevisionXMLToJSONInputFormat], hadoopConf)
    val context = new TaskAttemptContextImpl(hadoopConf, new TaskAttemptID())
    val reader= inputFormat.createRecordReader(split, context)

    reader.initialize(split, context)

    reader.nextKeyValue shouldEqual false

  }

}