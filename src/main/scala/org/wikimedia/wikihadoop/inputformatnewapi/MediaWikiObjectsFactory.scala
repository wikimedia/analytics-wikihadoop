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
    def setTimestamp(timestamp: String): T
    def setPageMetaData(page: V): T
    def setContributor(contributor: U): T
    def setMinor(minor: Boolean): T
    def setComment(comment: String): T
    def setBytes(bytes: Long): T
    def setText(text: String): T
    def setSha1(sha1: String): T
    def setParentId(parentId: Long): T
    def setModel(model: String): T
    def setFormat(format: String): T
  }

  trait MediaWikiContributor {
    def setId(id: Long): U
    def setUserText(userText: String): U
  }

  trait MediaWikiPageMetaData {
    def setId(id: Long): V
    def setNamespace(ns: Long): V
    def setTitle(title: String): V
    def setRedirectTitle(redirectTitle: String): V
    def addRestriction(restriction: String): V
  }

  def makeDummyRevision: T
  def makeDummyContributor: U
  def makeDummyPageMetaData: V

  def toText(revision: T): Text

}