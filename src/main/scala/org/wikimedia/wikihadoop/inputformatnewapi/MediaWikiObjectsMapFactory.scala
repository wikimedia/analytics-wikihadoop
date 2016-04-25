package org.wikimedia.wikihadoop.inputformatnewapi

import org.apache.hadoop.io.Text
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

class MediaWikiObjectsMapFactory extends MediaWikiObjectsFactory {

  type T = MediaWikiRevisionMap
  type U = MediaWikiContributorMap
  type V = MediaWikiPageMetaDataMap

  // Needed for json serialization
  implicit val formats = DefaultFormats

  abstract class MapBasedObject(val m: Map[String, Any]) {
    type M <: MapBasedObject
    def create(m: Map[String, Any]): M
    def push(kvPair: (String, Any)): M = create(this.m + kvPair)
  }

  class MediaWikiRevisionMap(m: Map[String, Any]) extends MapBasedObject(m) with MediaWikiRevision {
    type M =  MediaWikiRevisionMap
    override def create(m: Map[String, Any]) = new MediaWikiRevisionMap(m)
    override def setId(id: Long) = push("id" -> id)
    override def getId() = m("id").asInstanceOf[Long]
    override def setTimestamp(timestamp: String) = push("timestamp" -> timestamp)
    override def getTimestamp() = m("timestamp").asInstanceOf[String]
    override def setPageMetaData(page: V) = push("page" -> page.m)
    override def getPageMetaData() = m("page").asInstanceOf[V]
    override def setContributor(contributor: U) = push("user" -> contributor.m)
    override def getContributor() = m("user").asInstanceOf[U]
    override def setMinor(minor: Boolean) = push("minor" -> minor)
    override def getMinor() = m("minor").asInstanceOf[Boolean]
    override def setComment(comment: String) = push("comment" -> comment)
    override def getComment() = m("comment").asInstanceOf[String]
    override def setBytes(bytes: Long) = push("bytes" -> bytes)
    override def getBytes() = m("bytes").asInstanceOf[Long]
    override def setText(text: String) = push("text" -> text)
    override def getText() = m("text").asInstanceOf[String]
    override def setSha1(sha1: String) = push("sha1" -> sha1)
    override def getSha1() = m("sha1").asInstanceOf[String]
    override def setParentId(parentId: Long) = push("parent_id" -> parentId)
    override def getParentId() = m("parent_id").asInstanceOf[Long]
    override def setModel(model: String) = push("model" -> model)
    override def getModel() = m("model").asInstanceOf[String]
    override def setFormat(format: String) = push("format" -> format)
    override def getFormat() = m("format").asInstanceOf[String]
  }

  class MediaWikiContributorMap(m: Map[String, Any]) extends MapBasedObject(m) with MediaWikiContributor {
    type M =  MediaWikiContributorMap
    override def create(m: Map[String, Any]) = new MediaWikiContributorMap(m)
    override def setId(id: Long) = push("id" -> id)
    override def getId() = m("id").asInstanceOf[Long]
    override def setUserText(userText: String) = push("text" -> userText)
    override def getUserText() = m("text").asInstanceOf[String]
  }

  class MediaWikiPageMetaDataMap(m: Map[String, Any]) extends MapBasedObject(m) with MediaWikiPageMetaData {
    type M =  MediaWikiPageMetaDataMap
    override def create(m: Map[String, Any]) = new MediaWikiPageMetaDataMap(m)
    override def setId(id: Long) = push("id" -> id)
    override def getId() = m("id").asInstanceOf[Long]
    override def setNamespace(ns: Long) = push("namespace" -> ns)
    override def getNamespace() = m("namespace").asInstanceOf[Long]
    override def setTitle(title: String) = push("title" -> title)
    override def getTitle() = m("title").asInstanceOf[String]
    override def setRedirectTitle(redirectTitle: String) = push("redirect" -> redirectTitle)
    override def getRedirectTitle() = m("redirect").asInstanceOf[String]
    override def addRestriction(restriction: String) = push("restrictions" -> (m("restrictions").asInstanceOf[List[String]] :+ restriction))
    override def getRestrictions() = m("restrictions").asInstanceOf[Seq[String]]
  }

  def makeDummyRevision = new MediaWikiRevisionMap(Map(
    "id" -> -1L,
    "timestamp" -> "",
    "page" -> Map().asInstanceOf[Map[String, Any]],
    "user" -> Map().asInstanceOf[Map[String, Any]],
    "minor" -> false, // False by default
    "comment" -> "",
    "bytes" -> 0,
    "text" -> "",
    "sha1" -> "",
    "parent_id" -> -1,
    "model" -> "",
    "format" -> ""
  ))

  def makeDummyContributor = new MediaWikiContributorMap(Map("id" -> -1L, "text" -> ""))

  def makeDummyPageMetaData = new MediaWikiPageMetaDataMap(Map(
    "id" -> -1L,
    "namespace" -> -1L,
    "title" -> "",
    "redirect" -> "",
    "restrictions" -> List.empty[String]
  ))

  def toText(revision: MediaWikiRevisionMap): Text = new Text(Serialization.write(revision.m))

}


