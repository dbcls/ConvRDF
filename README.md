Converts a file in some RDF formats such as RDF/XML, JSON-LD, Turtle to it in N-Triples.

ConvRDF converts a file in a streaming fashion, so that it can handle huge numbers of triples (> 1 billion) without consuming a huge amount of memory.

This tool requires Apache Jena libraries. We tested it on the version 3.4.0.

This tool can be used as follows:

```$ java -jar ConvRDF.jar <file to be converted>```

A gziped file can be processed properly.

NOTICE: This tool uses Apache Jena libraries, which are released under the Apache License Version 2.0.
