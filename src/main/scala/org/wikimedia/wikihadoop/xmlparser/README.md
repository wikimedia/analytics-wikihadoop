# Mediawiki XML-dumps parser

This package contains code to parse mediawiki XML dumps (whether stub or full).

The parsing is done in the `MediawikiXMLParser` class, using WoodStox StreamReader.
This allowing not to load the entire XML tree in memory (wiki dumps file can be big !),
but rather stream-read it.

The parsed-objects (revision, page and user) are abstracted in the `MediawikiObjectsFactory`
trait. This abstraction allows to pick underlying representation to match the needs.
Currently provided implementations are `MediawikiObjectsMapFactory` using Map[String, Any]
and `MediawikiObjectsCaseClassesFactory` using scala case-classes.
