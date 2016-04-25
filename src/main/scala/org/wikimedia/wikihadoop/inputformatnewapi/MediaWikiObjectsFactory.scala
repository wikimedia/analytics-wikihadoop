package org.wikimedia.wikihadoop.inputformatnewapi

import org.apache.hadoop.io.Text

/**
 * Created by jo on 4/11/15.
 */

trait MediaWikiObjectsFactory {

  type T <: MediaWikiRevision
  type U <: MediaWikiContributor
  type V <: MediaWikiPageMetaData

  trait MediaWikiRevision {
    def setId(id: Long): T
    def getId(): Long
    def setTimestamp(timestamp: String): T
    def getTimestamp(): String
    def setPageMetaData(page: V): T
    def getPageMetaData(): V
    def setContributor(contributor: U): T
    def getContributor(): U
    def setMinor(minor: Boolean): T
    def getMinor(): Boolean
    def setComment(comment: String): T
    def getComment(): String
    def setBytes(bytes: Long): T
    def getBytes: Long
    def setText(text: String): T
    def getText(): String
    def setSha1(sha1: String): T
    def getSha1(): String
    def setParentId(parentId: Long): T
    def getParentId(): Long
    def setModel(model: String): T
    def getModel(): String
    def setFormat(format: String): T
    def getFormat: String
  }

  trait MediaWikiContributor {
    def setId(id: Long): U
    def getId(): Long
    def setUserText(userText: String): U
    def getUserText(): String
  }

  trait MediaWikiPageMetaData {
    def setId(id: Long): V
    def getId(): Long
    def setNamespace(ns: Long): V
    def getNamespace(): Long
    def setTitle(title: String): V
    def getTitle(): String
    def setRedirectTitle(redirectTitle: String): V
    def getRedirectTitle(): String
    def addRestriction(restriction: String): V
    def getRestrictions: Seq[String]
  }

  def makeDummyRevision: T
  def makeDummyContributor: U
  def makeDummyPageMetaData: V

  def toText(revision: T): Text

}