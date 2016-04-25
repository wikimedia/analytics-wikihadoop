package org.wikimedia.wikihadoop.inputformatnewapi

import org.apache.hadoop.io.Writable

/**
 * Created by jo on 11/19/15.
 */
trait KeyValueFactory[K <: Writable, V <: Writable, F <: MediaWikiObjectsFactory] {

  def newKey(): K
  def setKey(key: K, rev: F#MediaWikiRevision): Unit

  def newValue(): V
  def setValue(value: V, rev: F#MediaWikiRevision): Unit

  def filterKeyValue(key: K, value: V): Boolean = false
}
