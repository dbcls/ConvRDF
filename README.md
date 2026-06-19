# ConvRDF

Converts a file in some RDF formats such as RDF/XML, JSON-LD, Turtle to it in N-Triples.

ConvRDF converts a file in a streaming fashion, so that it can handle huge numbers of triples (> 1 billion) without consuming a huge amount of memory.

Compressed (.gz, .bz2, and .xz) files including .tar.gz can be processed properly.

__NOTICE__: This tool uses [Apache Jena](https://jena.apache.org/) and [Apache Commons](https://commons.apache.org/) libraries, which are released under the Apache License Version 2.0.
In addition, [XZ for Java](https://tukaani.org/xz/java.html) is used for processing XZ files.

## Requirements

- **Java 17 or later** (LTS recommended)
- Apache Jena 5.x (resolved automatically via Maven)

## Build

```sh
mvn -B package
# produces target/ConvRDF.jar (self-contained, executable)
```

## Usage

```
java -jar ConvRDF.jar [options] <file-or-directory>...
```

### Options

| Option       | Description                                                                                          |
| ------------ | ---------------------------------------------------------------------------------------------------- |
| `-r`         | Recursively process directories. Default: off.                                                       |
| `-c`         | Enable strict RDF syntax checking by Apache Jena. Default: off.                                      |
| `-i`         | Ignore lexical-level errors (bad IRIs, malformed literals, invalid Unicode escapes) with a warning, and continue. Structural syntax errors still abort the current file. Implies `-c`. |
| `-b <URI>`   | Override the base URI used to resolve relative IRIs. Default: derived from the input file path.      |
| `-o <file>`  | Output file. Extensions `.gz` and `.xz` trigger compression. Default: standard output.               |
| `-h`, `--help` | Show help and exit.                                                                                |

### Exit status

| Code | Meaning                                                                                          |
| ---- | ------------------------------------------------------------------------------------------------ |
| `0`  | All inputs were converted successfully.                                                          |
| `1`  | One or more errors occurred (invalid arguments, unreadable files, parse failures, or I/O errors). Errors are reported to stderr. |

### Examples

```sh
# Convert a single Turtle file to N-Triples on stdout
java -jar ConvRDF.jar data.ttl > data.nt

# Convert an xz-compressed RDF/XML file, writing gzip-compressed N-Triples
java -jar ConvRDF.jar -o out.nt.gz ontology.owl.xz

# Recursively convert a tree, tolerating bad IRIs but reporting them
java -jar ConvRDF.jar -r -i -o all.nt.xz /data/rdf/

# Override the base URI used to resolve relative IRIs in a streamed input
java -jar ConvRDF.jar -b http://example.org/ontology -o out.nt input.owl.xz
```

---

## Migration notes (v1 -> v2)

- **Apache Jena upgraded to 5.x.** Requires Java 17 or later.
- **Streaming pipeline rewritten.** The old `PipedRDFIterator` + thread-based
  producer/consumer pattern has been replaced with a direct `StreamRDF`
  pipeline. Memory use is now constant regardless of input size.
- **Base URI is set explicitly** when reading from compressed streams or tar
  entries. This fixes the previous `{E211} Base URI is null` failure on
  RDF/XML inputs that contain relative IRIs (for example `rdf:about=""`)
  when the input was a `.xz` / `.gz` / `.bz2` file.
- **New option `-i`** ignores lexical-level errors (bad IRIs, malformed
  literals, invalid Unicode escapes) with a warning, while still aborting
  on structural syntax errors.
- **New option `-b <URI>`** overrides the auto-derived base URI.
- **Exit status is now well-defined**: `0` on full success, `1` if any error
  occurred. Output is flushed and closed cleanly so `.gz` / `.xz` trailers
  are always written.