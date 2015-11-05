package org.wikimedia.wikihadoop.inputformatnewapi

import java.io.ByteArrayInputStream

import org.scalatest.BeforeAndAfterEach


class MediaWikiXMLParserSpec extends TimedSpec with BeforeAndAfterEach {

  val nbRepeat = 1000

  val contributor = "<contributor><id>1</id><username>Foobar</username></contributor>"

  val pageMetadata = "<page><id>1</id><title>Page title</title><restrictions>restriction 1</restrictions><restrictions>restriction 2</restrictions><redirect>test resdirect title</redirect></page>"

  val revision = "<revision><id>1</id><parentid>2</parentid><timestamp>2001-01-15T13:15:00Z</timestamp><contributor><username>Foobar</username></contributor><comment>test comment</comment><text>Test text\nWith new line.</text><sha1>test sha1</sha1><minor /><model>test model</model><format>test format</format></revision>"

  "MediaWikiXMLParser" should "parse contributor" in {
    val mediaWikiXMLParser = new MediaWikiXMLParser[MediaWikiObjectsMapFactory](new MediaWikiObjectsMapFactory)
    @transient var inputSeq: Seq[String] = Seq()
    for (i <- 1 to nbRepeat)
      inputSeq :+= contributor

    for (i <- 1 to 10) {
      /*println("Page metadatas native XML parsing - Slow")
      time {
        for (pageMetadataXML <- inputSeq) {
          val pageMetadataMap = mediaWikiXMLParser.parsePageMetaData_native(new ByteArrayInputStream(pageMetadataXML.getBytes("UTF-8")))
        }
      }
      println()*/

      println("Contributor woodstox stream XML parsing  with loop")
      time {
        for (pageMetadataXML <- inputSeq) {
          val contributorMap = mediaWikiXMLParser.parseContributor_map(new ByteArrayInputStream(pageMetadataXML.getBytes("UTF-8")))
        }
      }
      println()

      println("Contributor woodstox stream XML parsing recursively")
      time {
        for (pageMetadataXML <- inputSeq) {
          val contributorMap = mediaWikiXMLParser.parseContributor(new ByteArrayInputStream(pageMetadataXML.getBytes("UTF-8")))
        }
      }
      println()
    }
  }

  "MediaWikiXMLParser" should "parse page metadata" in {
    @transient var inputSeq: Seq[String] = Seq()
    for (i <- 1 to nbRepeat)
      inputSeq :+= pageMetadata

    for (i <- 1 to 10) {
      /*println("Page metadatas native XML parsing - Slow")
      time {
        for (pageMetadataXML <- inputSeq) {
          val pageMetadataMap = mediaWikiXMLParser.parsePageMetaData_native(new ByteArrayInputStream(pageMetadataXML.getBytes("UTF-8")))
        }
      }
      println()*/

      println("Page metadatas woodstox stream XML parsing with loop")
      time {
        val mediaWikiXMLParser = new MediaWikiXMLParser[MediaWikiObjectsMapFactory](new MediaWikiObjectsMapFactory)
        for (pageMetadataXML <- inputSeq) {
          val pageMetadataMap = mediaWikiXMLParser.parsePageMetaData_map(new ByteArrayInputStream(pageMetadataXML.getBytes("UTF-8")))
        }
      }
      println()

      println("Page metadatas woodstox stream XML parsing recursively")
      time {
        val mediaWikiXMLParser = new MediaWikiXMLParser[MediaWikiObjectsMapFactory](new MediaWikiObjectsMapFactory)
        for (pageMetadataXML <- inputSeq) {
          val pageMetadataMap = mediaWikiXMLParser.parsePageMetaData(new ByteArrayInputStream(pageMetadataXML.getBytes("UTF-8")))
        }
      }
      println()
    }
  }

  "MediaWikiXMLParser" should "parse revision" in {
    @transient var inputSeq: Seq[String] = Seq()
    for (i <- 1 to nbRepeat)
      inputSeq :+= revision

    println("Revisions woodstox stream XML parsing with loop")
    time {
      val mediaWikiXMLParser = new MediaWikiXMLParser[MediaWikiObjectsMapFactory](new MediaWikiObjectsMapFactory)
      for (revisionXML <- inputSeq) {
        val revisionMap = mediaWikiXMLParser.parseRevision_map(new ByteArrayInputStream(revisionXML.getBytes("UTF-8")),
          Map().asInstanceOf[Map[String, Any]])
      }
    }
    println()
    println("Revisions woodstox stream XML parsing recursively")
    time {
      val mediaWikiXMLParser = new MediaWikiXMLParser[MediaWikiObjectsMapFactory](new MediaWikiObjectsMapFactory)
      for (revisionXML <- inputSeq) {
        val revisionMap = mediaWikiXMLParser.parseRevision(
          mediaWikiXMLParser.initializeXmlStreamReader(new ByteArrayInputStream(revisionXML.getBytes("UTF-8"))),
          mediaWikiXMLParser.objectsFactory.makeDummyPageMetaData)
      }
    }
    println()
  }

}