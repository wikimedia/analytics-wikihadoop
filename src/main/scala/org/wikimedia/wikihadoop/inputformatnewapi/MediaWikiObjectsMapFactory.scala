package org.wikimedia.wikihadoop.inputformatnewapi

object MediaWikiObjectsMapFactory extends MediaWikiObjectsFactory {
  type T = MediaWikiRevisionMap
  type U = MediaWikiContributorMap
  type V = MediaWikiPageMetaDataMap

  abstract class MapBasedObject(val m: Map[String, Any]) {
    type M <: MapBasedObject
    def create(m: Map[String, Any]): M
    def push(kvPair: (String, Any)): M = create(this.m + kvPair)
  }

  class MediaWikiRevisionMap(m: Map[String, Any]) extends MapBasedObject(m) with MediaWikiRevision {
    type M =  MediaWikiRevisionMap
    override def create(m: Map[String, Any]) = new MediaWikiRevisionMap(m)
    override def setId(id: Long) = push("id" -> id)
    override def setTimestamp(timestamp: String) = push("timestamp" -> timestamp)
    override def setPageMetaData(page: V) = push("page" -> page.m)
    override def setContributor(contributor: U) = push("contributor" -> contributor.m)
    override def setMinor(minor: Boolean) = push("minor" -> minor)
    override def setComment(comment: String) = push("comment" -> comment)
    override def setBytes(bytes: Long) = push("bytes" -> bytes)
    override def setText(text: String) = push("text" -> text)
    override def setSha1(sha1: String) = push("sha1" -> sha1)
    override def setParentId(parentId: Long) = push("parent_id" -> parentId)
    override def setModel(model: String) = push("model" -> model)
    override def setFormat(format: String) = push("format" -> format)
  }

  class MediaWikiContributorMap(m: Map[String, Any]) extends MapBasedObject(m) with MediaWikiContributor {
    type M =  MediaWikiContributorMap
    override def create(m: Map[String, Any]) = new MediaWikiContributorMap(m)
    override def setId(id: Long) = push("id" -> id)
    override def setUserText(userText: String) = push("user_text" -> userText)
  }

  class MediaWikiPageMetaDataMap(m: Map[String, Any]) extends MapBasedObject(m) with MediaWikiPageMetaData {
    type M =  MediaWikiPageMetaDataMap
    override def create(m: Map[String, Any]) = new MediaWikiPageMetaDataMap(m)
    override def setId(id: Long) = push("id" -> id)
    override def setNamespace(ns: Long) = push("namespace" -> ns)
    override def setTitle(title: String) = push("title" -> title)
    override def setRedirectTitle(redirectTitle: String) = push("redirect_title" -> redirectTitle)
    override def addRestriction(restriction: String) = push("restrictions" -> (m("restrictions").asInstanceOf[List[String]] :+ restriction))
  }

  def makeDummyRevision = new MediaWikiRevisionMap(Map(
    "id" -> -1L,
    "timestamp" -> "",
    "page" -> Map().asInstanceOf[Map[String, Any]],
    "contributor" -> Map().asInstanceOf[Map[String, Any]],
    "minor" -> false, // False by default
    "comment" -> "",
    "bytes" -> 0,
    "text" -> "",
    "sha1" -> "",
    "parent_id" -> -1,
    "model" -> "",
    "format" -> ""
  ))

  def makeDummyContributor = new MediaWikiContributorMap(Map("id" -> -1L, "user_text" -> ""))

  def makeDummyPageMetaData = new MediaWikiPageMetaDataMap(Map(
    "id" -> -1L,
    "namespace" -> -1L,
    "title" -> "",
    "redirect_title" -> "",
    "restrictions" -> List.empty[String]
  ))

}


