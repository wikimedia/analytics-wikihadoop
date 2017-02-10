# Mediawiki XML-dumps hadoop input format (new API)

This package contains code providing hadoop InputFormat for parsing
mediawiki XML-dumps. This package uses code from the
`org.mediawiki.wikihadoop.xmlparser` package.

The `MediawikiXMLRevisionInputFormat` is the core abstract class of this package.
It contains all the code needed to process XML-dumps with the exception of the
functions defined in the `KeyValueFactory` trait.
The abstract functions defined in `KeyValueFactory` are the building of  the record
key and value out of parsed data, and a filtering function.
Having them abstract allows to easily instantiate different InputFormat in term of
generated key-value types and datasets without changing the core class.
The types parameters to provide to the core class are:
* K - The type of the record  key
* V  - The type of the record value
* MwObjectsFactory - The type of the class implementing `MediawikiObjectsFactory`
  (see `xmlparser` package) for mediawiki-objects manipulation

The `ByteMatcher` class and the `SeekableInputStream` class are low-level stream reading
and matching utilities, used by the core class to navigate the text-stream. 
 
Finally, using the `MediawikiXMLRevisionInputFormat` class as base and implementing the
missing functions from `KeyValueFactory`, a set of XML-to-JSON InputFormat classes are
provided in the `MediawikiXMLRevisionToJSONInputFormats` file.
