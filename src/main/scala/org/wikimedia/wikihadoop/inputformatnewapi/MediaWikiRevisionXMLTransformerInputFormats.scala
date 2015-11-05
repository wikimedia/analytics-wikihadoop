package org.wikimedia.wikihadoop.inputformatnewapi


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

