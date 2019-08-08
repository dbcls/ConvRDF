# ConvRDF

Converts a file in some RDF formats such as RDF/XML, JSON-LD, Turtle to it in N-Triples.

ConvRDF converts a file in a streaming fashion, so that it can handle huge numbers of triples (> 1 billion) without consuming a huge amount of memory.

This tool requires Apache Jena libraries. We tested it on the version 3.4.0.

This tool can be used as follows:

```$ java -jar ConvRDF.jar [-r|-c] <a file or a directory which contains files to be converted>```

Compressed (.gz, .bz2, and .xz) files including .tar.gz can be processed properly.
### Options:
* -r: recursively process directories. Default: not recursive.
* -c: enable RDF syntax cheking by Apache Jena (RDFParserBuilder). Default: disable.

__NOTICE__: This tool uses [Apache Jena](https://jena.apache.org/) and [Apache Commons](https://commons.apache.org/) libraries, which are released under the Apache License Version 2.0.
In addition, [XZ for Java](https://tukaani.org/xz/java.html) is used for processing XZ files.
