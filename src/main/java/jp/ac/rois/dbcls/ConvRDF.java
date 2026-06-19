/*
 * This code is derived from arq.examples.riot.ExRIOT_6, which is
 * distributed under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Database Center for Life Science (DBCLS) has developed this code
 * and releases it under MIT style license.
 */

package jp.ac.rois.dbcls;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorOutputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.system.ErrorHandler;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.riot.system.StreamRDFWriter;

/**
 * Streaming RDF format converter.
 *
 * Reads RDF data in any language Jena recognizes (Turtle, N-Triples, N-Quads,
 * TriG, RDF/XML, JSON-LD, ...), optionally inside .gz / .bz2 / .xz / .tar
 * archives, and writes the content as N-Triples (for triple languages) or
 * N-Quads (for quad languages) without loading the whole graph into memory.
 *
 * Run with no arguments or with -h for usage.
 */
public class ConvRDF {

    static { LogCtl.setCmdLogging(); }

    // ---- CLI state ----
    static boolean recursive;
    static boolean checking;
    static boolean ignoreLexical;
    static String  baseUriOverride;
    static OutputStream out;
    static boolean hadError;

    // =========================================================
    // Core conversion: StreamRDF direct pipeline (parser -> writer)
    // =========================================================

    /**
     * Convert one RDF input stream to the output, streaming triples/quads
     * directly from the parser to the writer without intermediate storage.
     *
     * @param in           input stream containing RDF data
     * @param inputLang    RDF language of the input
     * @param baseUri      base URI for resolving relative IRIs; may be null
     * @param sourceLabel  human-readable label for error messages
     */
    private static void convert(InputStream in, Lang inputLang,
                                String baseUri, String sourceLabel) {
        if (inputLang == null) {
            System.err.println("No RDF parser for: " + sourceLabel);
            hadError = true;
            return;
        }

        RDFFormat outputFormat = RDFLanguages.isQuads(inputLang)
                ? RDFFormat.NQUADS_UTF8
                : RDFFormat.NTRIPLES_UTF8;

        StreamRDF writer = StreamRDFWriter.getWriterStream(out, outputFormat);

        ErrorHandler handler = ignoreLexical
                ? lenientLexicalHandler(sourceLabel)
                : ErrorHandlerFactory.errorHandlerStd(null);

        // -i implies that the parser must validate IRIs/literals so we can
        // detect (and downgrade) lexical-level problems. -c also turns it on.
        boolean doChecking = checking || ignoreLexical;

        RDFParserBuilder pb = RDFParser.source(in)
                .lang(inputLang)
                .checking(doChecking)
                .errorHandler(handler);

        if (baseUri != null) {
            pb.base(baseUri);
        }

        writer.start();
        try {
            pb.parse(writer);
        } catch (RiotException e) {
            System.err.println("Parse error in \"" + sourceLabel + "\": " + e.getMessage());
            hadError = true;
        } finally {
            writer.finish();
        }
    }

    /**
     * Custom error handler that demotes lexical-level problems (bad IRIs,
     * malformed literals, invalid Unicode escapes, ...) to warnings and lets
     * parsing continue, while still aborting on structural syntax errors.
     *
     * Limitation: when a lexical defect breaks tokenization (e.g. a raw space
     * inside an IRI), the parser may report it as a structural error or throw
     * directly. Such cases cannot be reliably downgraded here.
     */
    private static ErrorHandler lenientLexicalHandler(final String sourceLabel) {
        return new ErrorHandler() {
            private boolean looksLexical(String msg) {
                if (msg == null) return false;
                String m = msg.toLowerCase();
                return m.contains("iri")
                    || m.contains("uri")
                    || m.contains("unicode")
                    || m.contains("escape")
                    || m.contains("codepoint")
                    || m.contains("character")
                    || m.contains("lexical")
                    || m.contains("language tag")
                    || m.contains("datatype")
                    || m.contains("malformed literal");
            }

            @Override
            public void warning(String msg, long line, long col) {
                System.err.println("Warning [" + sourceLabel + " " + line + ":" + col + "] " + msg);
            }

            @Override
            public void error(String msg, long line, long col) {
                if (looksLexical(msg)) {
                    System.err.println("Lexical error ignored [" + sourceLabel
                            + " " + line + ":" + col + "] " + msg);
                    // do not throw -- parser continues
                } else {
                    throw new RiotException("Syntax error [" + line + ":" + col + "] " + msg);
                }
            }

            @Override
            public void fatal(String msg, long line, long col) {
                throw new RiotException("Fatal [" + line + ":" + col + "] " + msg);
            }
        };
    }

    // =========================================================
    // Base URI derivation
    // =========================================================

    /** Build a base URI from a real file path, stripping any compression extension. */
    private static String deriveBaseUri(String filename) {
        if (baseUriOverride != null) return baseUriOverride;
        String logical = stripCompressionExt(filename);
        try {
            return Paths.get(logical).toAbsolutePath().toUri().toString();
        } catch (Exception e) {
            return null;
        }
    }

    /** Build a synthetic base URI for a tar entry (no real path available). */
    private static String deriveBaseUriForTarEntry(String entryName) {
        if (baseUriOverride != null) return baseUriOverride;
        return "file:///tar-entry/" + stripCompressionExt(entryName);
    }

    private static String stripCompressionExt(String name) {
        String ext = FilenameUtils.getExtension(name).toLowerCase();
        if (ext.equals("gz") || ext.equals("bz2") || ext.equals("xz")) {
            return FilenameUtils.removeExtension(name);
        }
        return name;
    }

    // =========================================================
    // File dispatching (compression / tar handling)
    // =========================================================

    private static void dispatch(String filename) {
        InputStream is = null;
        try {
            String fext = FilenameUtils.getExtension(filename).toLowerCase();
            FileInputStream fis = new FileInputStream(filename);
            String nameForLang;

            switch (fext) {
                case "gz":
                    is = new GzipCompressorInputStream(fis);
                    nameForLang = FilenameUtils.removeExtension(filename);
                    break;
                case "bz2":
                    is = new BZip2CompressorInputStream(fis);
                    nameForLang = FilenameUtils.removeExtension(filename);
                    break;
                case "xz":
                    is = new XZCompressorInputStream(fis);
                    nameForLang = FilenameUtils.removeExtension(filename);
                    break;
                case "":
                    fis.close();
                    return;
                default:
                    is = fis;
                    nameForLang = filename;
            }

            // Handle nested tar (foo.tar.gz, foo.tar.xz, ...)
            if (FilenameUtils.getExtension(nameForLang).equalsIgnoreCase("tar")) {
                procTar(is);
                return;
            }

            Lang lang = RDFLanguages.filenameToLang(nameForLang);
            if (lang == null) {
                System.err.println("No RDF parser for: " + filename);
                hadError = true;
                return;
            }
            System.err.println("File: " + filename + " (" + lang.getName() + ")");
            convert(is, lang, deriveBaseUri(filename), filename);
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
            hadError = true;
        } catch (IOException e) {
            System.err.println("IO Exception: " + e.getMessage());
            hadError = true;
        } finally {
            if (is != null) {
                try { is.close(); } catch (IOException ignored) {}
            }
        }
    }

    private static void procTar(InputStream ins) {
        try (TarArchiveInputStream tar = new TarArchiveInputStream(ins)) {
            TarArchiveEntry entry;
            while ((entry = tar.getNextEntry()) != null) {
                if (entry.isDirectory()) continue;
                String entryName = entry.getName();
                String fext = FilenameUtils.getExtension(entryName).toLowerCase();
                String nameForLang = (fext.equals("gz") || fext.equals("bz2") || fext.equals("xz"))
                        ? FilenameUtils.removeExtension(entryName)
                        : entryName;
                Lang lang = RDFLanguages.filenameToLang(nameForLang);
                if (lang == null) continue;

                System.err.println("Tar entry: " + entryName + " (" + lang.getName() + ")");
                InputStream entryIn = CloseShieldInputStream.wrap(tar);
                InputStream decoded;
                switch (fext) {
                    case "gz":  decoded = new GzipCompressorInputStream(entryIn); break;
                    case "bz2": decoded = new BZip2CompressorInputStream(entryIn); break;
                    case "xz":  decoded = new XZCompressorInputStream(entryIn); break;
                    default:    decoded = entryIn;
                }
                convert(decoded, lang, deriveBaseUriForTarEntry(entryName), entryName);
            }
        } catch (IOException e) {
            System.err.println("Error processing tar: " + e.getMessage());
            hadError = true;
        }
    }

    private static void processRecursively(File dir) {
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File f : files) {
            if (f.getName().startsWith(".")) continue;
            if (f.isFile()) {
                if (f.getName().endsWith(".taz")) {
                    try {
                        procTar(new GzipCompressorInputStream(new FileInputStream(f.getPath())));
                    } catch (IOException e) {
                        System.err.println("IO Exception: " + e.getMessage());
                        hadError = true;
                    }
                } else {
                    dispatch(f.getPath());
                }
            } else if (f.isDirectory() && recursive) {
                processRecursively(f);
            }
        }
    }

    // =========================================================
    // Help
    // =========================================================

    private static void showHelp() {
        System.out.println(
            "Usage: java -jar ConvRDF.jar [options] <file-or-directory>...\n" +
            "\n" +
            "Converts RDF files (optionally compressed with gzip/bzip2/xz, or packed\n" +
            "in tar archives) to N-Triples or N-Quads on the fly, without loading the\n" +
            "entire graph into memory.\n" +
            "\n" +
            "Options:\n" +
            "  -r            Recursively process directories. Default: off.\n" +
            "  -c            Enable strict RDF syntax checking by Apache Jena.\n" +
            "                Default: off.\n" +
            "  -i            Ignore lexical-level errors (bad IRIs, malformed literals,\n" +
            "                invalid Unicode escapes) with a warning, and continue.\n" +
            "                Structural syntax errors still abort the current file.\n" +
            "                Implies -c internally so that lexical issues are detected.\n" +
            "  -b <URI>      Override the base URI used to resolve relative IRIs.\n" +
            "                Default: derived from the input file path.\n" +
            "  -o <file>     Output file. Extensions .gz and .xz trigger compression.\n" +
            "                Default: standard output.\n" +
            "  -h, --help    Show this help and exit.\n" +
            "\n" +
            "Exit status:\n" +
            "  0  All inputs were converted successfully.\n" +
            "  1  One or more errors occurred (invalid arguments, unreadable files,\n" +
            "     parse failures, or I/O errors). Errors are reported to stderr.");
    }

    // =========================================================
    // main
    // =========================================================

    public static void main(String[] args) {
        recursive       = false;
        checking        = false;
        ignoreLexical   = false;
        baseUriOverride = null;
        out             = System.out;
        hadError        = false;

        int idx = 0;
        while (idx < args.length && args[idx].startsWith("-")) {
            String opt = args[idx];
            if (opt.equals("-r")) {
                recursive = true;
            } else if (opt.equals("-c")) {
                checking = true;
            } else if (opt.equals("-i")) {
                ignoreLexical = true;
            } else if (opt.equals("-b")) {
                idx++;
                if (idx >= args.length) {
                    System.err.println("Error: -b requires a URI argument.");
                    System.exit(1);
                }
                baseUriOverride = args[idx];
            } else if (opt.equals("-o")) {
                idx++;
                if (idx >= args.length) {
                    System.err.println("Error: -o requires a filename argument.");
                    System.exit(1);
                }
                try {
                    OutputStream fos = new FileOutputStream(args[idx]);
                    if (args[idx].endsWith(".gz")) {
                        fos = new GZIPOutputStream(fos);
                    } else if (args[idx].endsWith(".xz")) {
                        fos = new XZCompressorOutputStream(fos);
                    }
                    out = new BufferedOutputStream(fos);
                } catch (IOException e) {
                    System.err.println("Cannot open output: " + e.getMessage());
                    System.exit(1);
                }
            } else if (opt.equals("-h") || opt.equals("--help")) {
                showHelp();
                System.exit(0);
            } else {
                System.err.println("Unknown option: " + opt);
                showHelp();
                System.exit(1);
            }
            idx++;
        }

        if (idx == args.length) {
            System.err.println("Error: no input files specified.");
            showHelp();
            System.exit(1);
        }

        for (int i = idx; i < args.length; i++) {
            File file = new File(args[i]);
            if (!(file.exists() && file.canRead())) {
                System.err.println("Cannot read: " + file);
                hadError = true;
                continue;
            }
            System.err.println("Reading: " + file);
            try {
                if (file.isFile()) {
                    if (file.getName().endsWith(".taz")) {
                        procTar(new GzipCompressorInputStream(new FileInputStream(args[i])));
                    } else if (file.getName().startsWith(".")) {
                        continue;
                    } else {
                        dispatch(args[i]);
                    }
                } else if (file.isDirectory()) {
                    processRecursively(file);
                }
            } catch (FileNotFoundException e) {
                System.err.println("File not found: " + e.getMessage());
                hadError = true;
            } catch (IOException e) {
                System.err.println("IO Exception: " + e.getMessage());
                hadError = true;
            }
        }

        // Close the output. Critical for .gz / .xz to write trailers/index.
        try {
            if (out != System.out) {
                out.close();
            } else {
                out.flush();
            }
        } catch (IOException e) {
            System.err.println("Error closing output: " + e.getMessage());
            hadError = true;
        }

        System.exit(hadError ? 1 : 0);
    }
}
