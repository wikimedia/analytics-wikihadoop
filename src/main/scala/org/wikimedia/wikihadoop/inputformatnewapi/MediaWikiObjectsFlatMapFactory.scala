package org.wikimedia.wikihadoop.inputformatnewapi

import org.apache.hadoop.io.Text
import org.json4s.jackson.Serialization


class MediaWikiObjectsFlatMapFactory extends MediaWikiObjectsMapFactory {

  def flatten_map_rec(m: Map[String, Any], r: Map[String, Any], header: String): Map[String, Any] = {
    m.foldLeft(r)((fm, kv) => {
      val (k, v) = kv
      if (! v.isInstanceOf[Map[String, Any]])
        fm + ((header + k) -> v)
      else
        flatten_map_rec(v.asInstanceOf[Map[String, Any]], fm, k + "_")
    })
  }


  override def toText(revision: MediaWikiRevisionMap): Text = {
    new Text(Serialization.write(flatten_map_rec(revision.m, Map(), "")))
  }



}


