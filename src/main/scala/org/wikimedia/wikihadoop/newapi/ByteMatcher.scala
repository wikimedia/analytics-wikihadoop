package org.wikimedia.wikihadoop.newapi

import java.io.InputStream

import org.apache.hadoop.fs.Seekable
import org.apache.hadoop.io.DataOutputBuffer

import scala.annotation.tailrec

/**
 * Modified from DBPedia distibuted extraction framework (https://github.com/dbpedia/distributed-extraction-framework)
 *
 * A class that operates mainly on SeekableInputStreams, iteratively reading chunks of data from an InputStream
 * depending upon a match pattern, through the method readUntilMatch().
 *
 * @param in InputStream to read binary data from
 * @param seeker Seekable for the InputStream "in" - used for keeping track of position in the InputStream
 */
class ByteMatcher(in: InputStream, seeker: Seekable)
{
  private var bytesRead: Long = 0
  private var lastMatchedPos: Long = -1
  private var currentPos: Long = -1

  def this(is: SeekableInputStream) = this(is, is)

  /**
   * @return number of bytes read
   */
  def getReadBytes: Long = bytesRead

  /**
   * @return current position in seeker
   */
  def getPos: Long = seeker.getPos

  /**
   * @return last position when a match was found
   */
  def getLastMatchedPos: Long = lastMatchedPos

  /**
   * @param len number of bytes to skip
   */
  def skip(len: Long)
  {
    in.skip(len)
    bytesRead += len
  }

  /**
   * Reads the InputStream until a match is found or "end" number of bytes is reached.
   *
   * @param textPattern String to match against
   * @param end number of bytes to read till - checked against seeker
   * @return Boolean true if a match was found, false if EOF was found or stopping point "end" was crossed.
   */
  def readUntilMatch(textPattern: String, end: Long): Boolean =
  {
    readUntilMatch(textPattern.getBytes("UTF-8"), 0, Some(end))
  }

  /**
   * Reads the InputStream while writing to a buffer, until a match is found or "end" number of bytes is reached.
   *
   * @param textPattern String to match against
   * @param end number of bytes to read till - checked against seeker
   * @param outputBuffer DataOutputBuffer where the data being read is written to
   * @return Boolean true if a match was found, false if EOF was found or stopping point "end" was crossed.
   */
  def readUntilMatch(textPattern: String, end: Long, outputBuffer: Option[DataOutputBuffer]): Boolean =
  {
    readUntilMatch(textPattern.getBytes("UTF-8"), 0, Some(end), outputBuffer)
  }

  /**
   * Reads the InputStream until a match is found or "end" number of bytes is reached.
   *
   * @param bytePattern Byte array to match against
   * @param end number of bytes to read till - checked against seeker
   * @return Boolean true if a match was found, false if EOF was found or stopping point "end" was crossed.
   */
  def readUntilMatch(bytePattern: Array[Byte], end: Long): Boolean =
  {
    readUntilMatch(bytePattern, 0, Some(end))
  }

  /**
   * Reads the InputStream while writing to a buffer, until a match is found or "end" number of bytes is reached.
   *
   * @param bytePattern Byte array to match against
   * @param end number of bytes to read till - checked against seeker
   * @param outputBuffer DataOutputBuffer where the data being read is written to
   * @return Boolean true if a match was found, false if EOF was found or stopping point "end" was crossed.
   */
  def readUntilMatch(bytePattern: Array[Byte], end: Long, outputBuffer: Option[DataOutputBuffer]): Boolean =
  {
    readUntilMatch(bytePattern, 0, Some(end), outputBuffer)
  }

  /**
   * Reads the InputStream while writing to a buffer, until a match is found or the end of stream is reached.
   *
   * @param bytePattern Byte array to match against
   * @param outputBuffer DataOutputBuffer where the data being read is written to
   * @return Boolean true if a match was found, false if EOF was found or stopping point "end" was crossed.
   */
  def readUntilMatch(bytePattern: Array[Byte], outputBuffer: Option[DataOutputBuffer]): Boolean =
  {
    readUntilMatch(bytePattern, 0, None, outputBuffer)
  }

  @tailrec private def readUntilMatch(matchBytes: Array[Byte], matchIter: Int, end: Option[Long], outputBuffer: Option[DataOutputBuffer] = None): Boolean =
  {
    var i = matchIter
    val b: Int = this.in.read
    // EOF at the beginning
    if (b == -1) return false

    this.bytesRead += 1

    // Save to the buffer, if any provided
    outputBuffer.foreach(_.write(b))

    // Check if we're matching
    if (b == matchBytes(i))
    {
      i += 1
      // Whole of matchBytes matched successfully?
      if (i >= matchBytes.length) return true
    }
    else
    {
      // If not matched, start afresh and increment position.
      i = 0
      if (this.currentPos != this.getPos)
      {
        this.lastMatchedPos = this.currentPos
        this.currentPos = this.getPos
      }
    }

    // If needed (end defined), see if we've passed the stop point
    if (end.isDefined && i == 0 && this.seeker.getPos >= end.get) return false

    // Keep reading
    readUntilMatch(matchBytes, i, end, outputBuffer)
  }

}
