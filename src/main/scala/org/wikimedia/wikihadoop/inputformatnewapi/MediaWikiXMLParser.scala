package org.wikimedia.wikihadoop.inputformatnewapi

import java.io.InputStream
import javax.xml.stream.{XMLStreamConstants, XMLInputFactory}

import org.codehaus.stax2.{XMLStreamReader2, XMLInputFactory2}

import scala.annotation.tailrec


/**
 * Parses xml chunks of data from wikimedia dumps.
 * Builds revision objects using MediaWikiRevisionFactory trait.
 * Page metadata parsing expects pages without revisions or uploads.
 */
class MediaWikiXMLParser[T <: MediaWikiObjectsFactory](val objectsFactory: T) {

  val xmlInputFactory = XMLInputFactory.newInstance match {
    case xmlInputFactory2: XMLInputFactory2 => xmlInputFactory2
    case _ => throw new ClassCastException
  }

  def initializeXmlStreamReader(inputStreamReader: InputStream): XMLStreamReader2 = {
    xmlInputFactory.createXMLStreamReader(inputStreamReader) match {
      case xmlStreamReader2: XMLStreamReader2 => xmlStreamReader2
      case _ => throw new ClassCastException
    }
  }

  def parsePageMetaData_map(inputStreamReader: InputStream): Map[String, Any] =
    parsePageMetaData_map(initializeXmlStreamReader(inputStreamReader))

  def parsePageMetaData_map(xmlStreamReader: XMLStreamReader2): Map[String, Any] = {
    var pageMetaDataFieldsMap: Map[String, Any] = Map(
      "id" -> -1L,
      "namespace" -> -1L,
      "title" -> "",
      "redirect_title" -> "",
      "restrictions" -> Seq.empty[String]
    )

    while (xmlStreamReader.hasNext) {
      val eventType: Int = xmlStreamReader.next
      eventType match {
        case XMLStreamConstants.START_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "title" => pageMetaDataFieldsMap += ("title" -> xmlStreamReader.getElementText)
            case "ns" => pageMetaDataFieldsMap += ("namespace" -> xmlStreamReader.getElementText.toLong)
            case "id" => pageMetaDataFieldsMap += ("id" -> xmlStreamReader.getElementText.toLong)
            case "redirect" => pageMetaDataFieldsMap += ("redirect_title" -> xmlStreamReader.getAttributeValue(null, "title"))
            case "restrictions" => pageMetaDataFieldsMap +=
              ("restrictions" -> (pageMetaDataFieldsMap("restrictions").asInstanceOf[Seq[String]] ++ xmlStreamReader.getElementText))
            case _ => ()
          }
        }
        case _ => ()
      }
    }
    // Return map
    return pageMetaDataFieldsMap
  }

  def parseContributor_map(inputStreamReader: InputStream): Map[String, Any] =
    parseContributor_map(initializeXmlStreamReader(inputStreamReader))

  def parseContributor_map(xmlStreamReader: XMLStreamReader2): Map[String, Any] = {
    var fieldsMap: Map[String, Any] = Map("id" -> -1L, "user_text" -> "")

    while (xmlStreamReader.hasNext) {
      val eventType: Int = xmlStreamReader.next
      eventType match {
        case XMLStreamConstants.START_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "id" => fieldsMap += ("id" -> xmlStreamReader.getElementText.toLong)
            case "username" | "ip" => fieldsMap += ("user_text" -> xmlStreamReader.getElementText)
            case _ => ()
          }
        }
        case XMLStreamConstants.END_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "contributor" => return fieldsMap
            case _ => ()
          }
        }
        case _ => ()
      }
    }
    return fieldsMap
  }

  def parseRevision_map(inputStreamReader: InputStream, page: Map[String, Any]): Map[String, Any] =
    parseRevision_map(initializeXmlStreamReader(inputStreamReader), page)

  def parseRevision_map(xmlStreamReader: XMLStreamReader2, page: Map[String, Any]): Map[String, Any] = {
    // Initialize filed maps
    var fieldsMap: Map[String, Any] = Map(
      "id" -> -1L,
      "timestamp" -> "",
      "page" -> page,
      "contributor" -> Map().asInstanceOf[Map[String, Any]],
      "minor" -> false, // False by default
      "comment" -> "",
      "bytes" -> 0,
      "text" -> "",
      "sha1" -> "",
      "parent_id" -> -1,
      "model" -> "",
      "format" -> ""
    )
    while (xmlStreamReader.hasNext) {
      val eventType: Int = xmlStreamReader.next
      eventType match {
        case XMLStreamConstants.START_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "id" => fieldsMap += ("id" -> xmlStreamReader.getElementText.toLong)
            case "parentid" => fieldsMap += ("parent_id" -> xmlStreamReader.getElementText.toLong)
            case "timestamp" => fieldsMap += ("timestamp" -> xmlStreamReader.getElementText)
            case "contributor" => fieldsMap += ("contributor" -> parseContributor_map(xmlStreamReader))
            case "minor" => fieldsMap += ("minor" -> true)
            case "comment" => fieldsMap += ("comment" -> xmlStreamReader.getElementText)
            case "model" => fieldsMap += ("model" -> xmlStreamReader.getElementText)
            case "format" => fieldsMap += ("format" -> xmlStreamReader.getElementText)
            case "text" => {
              fieldsMap += ("text" -> xmlStreamReader.getElementText)
              fieldsMap += ("bytes" -> fieldsMap("text").asInstanceOf[String].getBytes("utf-8").length.toLong)
            }
            case "sha1" => fieldsMap += ("sha1" -> xmlStreamReader.getElementText)
            case _ => ()
          }
        }
        case XMLStreamConstants.END_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "revision" => return fieldsMap
            case _ => ()
          }
        }
        case _ => ()
      }
    }
    return fieldsMap
  }

  def parseContributor(inputStreamReader: InputStream): objectsFactory.U =
    parseContributor(initializeXmlStreamReader(inputStreamReader))

  def parseContributor(xmlStreamReader: XMLStreamReader2): objectsFactory.U =
    parseContributor_rec(xmlStreamReader, objectsFactory.makeDummyContributor)

  @tailrec private def parseContributor_rec(xmlStreamReader: XMLStreamReader2, contributor: objectsFactory.U): objectsFactory.U =
    if (! xmlStreamReader.hasNext) contributor
    else parseContributor_rec(xmlStreamReader, {
      xmlStreamReader.next match {
        case XMLStreamConstants.START_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "id" => contributor.setId(xmlStreamReader.getElementText.toLong)
            case "username" | "ip" => contributor.setUserText(xmlStreamReader.getElementText)
            case _ => contributor
          }
        }
        case XMLStreamConstants.END_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "contributor" => return contributor
            case _ => contributor
          }
        }
        case _ => contributor
      }
    })


  def parsePageMetaData(inputStreamReader: InputStream): objectsFactory.V =
    parsePageMetaData(initializeXmlStreamReader(inputStreamReader))

  def parsePageMetaData(xmlStreamReader: XMLStreamReader2): objectsFactory.V =
    parsePageMetaData_rec(xmlStreamReader, objectsFactory.makeDummyPageMetaData)

  @tailrec private def parsePageMetaData_rec(xmlStreamReader: XMLStreamReader2, pageMetaData: objectsFactory.V): objectsFactory.V =
    if (! xmlStreamReader.hasNext) pageMetaData
    else parsePageMetaData_rec(xmlStreamReader, {
      xmlStreamReader.next match {
        case XMLStreamConstants.START_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "title" => pageMetaData.setTitle(xmlStreamReader.getElementText)
            case "ns" => pageMetaData.setNamespace(xmlStreamReader.getElementText.toLong)
            case "id" => pageMetaData.setId(xmlStreamReader.getElementText.toLong)
            case "redirect" => pageMetaData.setRedirectTitle(xmlStreamReader.getAttributeValue(null, "title"))
            case "restrictions" => pageMetaData.addRestriction(xmlStreamReader.getElementText)
            case _ => pageMetaData
          }
        }
        case _ => pageMetaData
      }
    })

  def parseRevision(inputStreamReader: InputStream, pageMetaData: objectsFactory.V): objectsFactory.T =
    parseRevision(initializeXmlStreamReader(inputStreamReader), pageMetaData)

  def parseRevision(xmlStreamReader: XMLStreamReader2, pageMetaData: objectsFactory.V): objectsFactory.T =
    parseRevision_rec(xmlStreamReader, objectsFactory.makeDummyRevision.setPageMetaData(pageMetaData))

  @tailrec private def parseRevision_rec(xmlStreamReader: XMLStreamReader2, revision: objectsFactory.T): objectsFactory.T =
    if (! xmlStreamReader.hasNext) revision
    else parseRevision_rec(xmlStreamReader, {
      xmlStreamReader.next match {
        case XMLStreamConstants.START_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "id" => revision.setId(xmlStreamReader.getElementText.toLong)
            case "parentid" => revision.setParentId(xmlStreamReader.getElementText.toLong)
            case "timestamp" => revision.setTimestamp(xmlStreamReader.getElementText)
            case "contributor" => revision.setContributor(parseContributor(xmlStreamReader))
            case "minor" => revision.setMinor(true)
            case "comment" => revision.setComment(xmlStreamReader.getElementText)
            case "model" => revision.setModel(xmlStreamReader.getElementText)
            case "format" => revision.setFormat(xmlStreamReader.getElementText)
            case "text" => {
              val text = xmlStreamReader.getElementText
              revision.setText(text).setBytes(text.getBytes("utf-8").length.toLong)
            }
            case "sha1" => revision.setSha1(xmlStreamReader.getElementText)
            case _ => revision
          }
        }
        case XMLStreamConstants.END_ELEMENT => {
          xmlStreamReader.getName.toString match {
            case "revision" => return revision
            case _ => revision
          }
        }
      case _ => revision
      }
    })

}
