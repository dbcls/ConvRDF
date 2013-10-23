Converts a file in RDF/XML to it in N-Triples.

ConvRDF converts a file in a streaming fashion, so that it can handle huge numbers of triples (> 1 billion) without consuming a huge amount of memory.

This tool requires Apache Jena. We tested it on the version 2.11.0.

This tool can be used as follows:

$ java -cp ./apache-jena-2.11.0/lib/*:./ConvRDF.jar jp.ac.rois.dbcls.ConvRDF &lt;file to be converted&gt;
