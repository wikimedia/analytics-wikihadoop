package org.wikimedia.wikihadoop.inputformatnewapi

import org.apache.hadoop.io.{Text, LongWritable}


/**
 * Modified from DBPedia distibuted extraction framework (https://github.com/dbpedia/distributed-extraction-framework)
 *
 * Hadoop InputFormat that splits a Wikipedia dump file into json revision strings
 * adding current page metadata as fields in the revision
 *
 * The WikiRevisionXMLToJSONRecordReader class inside outputs a Text as value and the starting position (byte) as key.
 *
 */
class MediaWikiRevisionXMLToJSONInputFormat extends MediaWikiRevisionXMLTransformerInputFormat[MediaWikiObjectsMapFactory](new MediaWikiObjectsMapFactory) {}

class MediaWikiRevisionXMLToFlatJSONInputFormat extends MediaWikiRevisionXMLTransformerInputFormat[MediaWikiObjectsFlatMapFactory](new MediaWikiObjectsFlatMapFactory) {}

class MediaWikiRevisionXMLToFlatJSONInputFormatPageIdKey
  extends MediaWikiRevisionXMLTransformerInputFormatAbstract
    [LongWritable, Text, MediaWikiObjectsFlatMapFactory](new MediaWikiObjectsFlatMapFactory) {
  override def newKey(): LongWritable = new LongWritable()
  override def setKey(key: LongWritable, rev: MediaWikiObjectsFlatMapFactory#MediaWikiRevision): Unit = {
    key.set(rev.getPageMetaData().getId())
  }

  override def newValue(): Text = new Text()
  override def setValue(value: Text, rev: MediaWikiObjectsFlatMapFactory#MediaWikiRevision): Unit = {
    value.set(objectsFactory.toText(rev.asInstanceOf[objectsFactory.T]))
  }
}