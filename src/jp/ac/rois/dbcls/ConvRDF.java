/*
 * This code is derived from arq.examples.riot.ExRIOT_6, which is 
 * distributed under the Apache License, Version 2.0.
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Database Center for Life Science (DBCLS) has developed this code
 * and releases it under MIT style license. 
 */

package jp.ac.rois.dbcls;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RiotNotFoundException;
import org.apache.jena.riot.RiotParseException;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.riot.system.ErrorHandlerFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.Graph;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.ext.com.google.common.io.CharStreams;
import org.apache.jena.graph.Factory;
import org.apache.jena.riot.RDFLanguages;

public class ConvRDF {

	static { LogCtl.setCmdLogging(); }
	static boolean recursive;
	static boolean checking;

	private static void issuer(String file) {
		System.err.println("File:" + file);
		issuer(file, null);
	}

	private static void issuer(Object reader, Lang lang) {
		if(reader.getClass() == StringReader.class && lang == null) return;
		final int interval = 10000;
		final int buffersize = 100000;
		final int pollTimeout = 300; // Poll timeout in milliseconds
		final int maxPolls = 1000;   // Max poll attempts

		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>(buffersize, false, pollTimeout, maxPolls);
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);

		ExecutorService executor = Executors.newSingleThreadExecutor();

		Runnable parser = new Runnable() {
			@Override
			public void run() {
				RDFParser parser_object;
				if(reader.getClass() == String.class) {
					parser_object = RDFParserBuilder
							.create()
							.errorHandler(ErrorHandlerFactory.errorHandlerDetailed())
							.source((String) reader)
							.checking(checking)
							.build();					
				}
				else if (reader.getClass() == StringReader.class) {
					parser_object = RDFParserBuilder
							.create()
							.errorHandler(ErrorHandlerFactory.errorHandlerDetailed())
							.source((StringReader) reader)
							.checking(checking)
							.lang(lang)
							.build();
				} else return;

				try{
					parser_object.parse(inputStream);
				}
				catch (RiotParseException e){
					String location = "";
					if(e.getLine() >= 0 && e.getCol() >= 0)
						location = " at the line: " + e.getLine() + " and the column: " + e.getCol();
					System.err.println("Parse error"
							+ location
							+ " in \""
							+ reader
							+ "\", and cannot parse this file anymore. Reason: "
							+ e.getMessage());
					inputStream.finish();
				}
				catch (RiotNotFoundException e){
					System.err.println("Format error for the file \"" + reader + "\": " + e.getMessage());
					inputStream.finish();
				}
			}
		};
		executor.submit(parser);

		int i = 0;
		Graph g = Factory.createDefaultGraph();
		try{
			while (iter.hasNext()) {
				Triple next = iter.next();
				g.add(next);
				i++;
				if(i % interval == 0){
					RDFDataMgr.write(System.out, g, RDFFormat.NTRIPLES_UTF8);
					g = Factory.createDefaultGraph();
				}
			}
			if(i % interval > 0){
				RDFDataMgr.write(System.out, g, RDFFormat.NTRIPLES_UTF8);
			}
		}
		catch (RiotException e){
			System.err.println("Riot Exception: " + e.getMessage());
		}
		executor.shutdown();
	}

	private static void dispatch(String filename){
		InputStream is = null;
		try {
			FileInputStream fis = new FileInputStream(filename);
			String fext = FilenameUtils.getExtension(filename);
			switch (fext) {
			case "gz":
				is = new GzipCompressorInputStream(fis);
				break;
			case "bz2": 
				is = new BZip2CompressorInputStream(fis);
				break;
			case "xz":
				is = new XZCompressorInputStream(fis);
				break;
			default:
				fis.close();
				issuer(filename);
				return;
			}
			String fwoext = FilenameUtils.removeExtension(filename);
			if(FilenameUtils.getExtension(fwoext).equals("tar")) {
				procTar(is);
			} else if (fext.equals("xz")) {
				Lang lang = RDFLanguages.filenameToLang(fwoext);
				issuer(new StringReader(CharStreams.toString(new InputStreamReader(is))), lang);
			} else {
				issuer(filename);
			}
		} catch (FileNotFoundException e) {
			System.err.println("File not found:" + e.getMessage());
		} catch (IOException e) {
			System.err.println("IO Exception:" + e.getMessage());
		}
	}

	private static void procTar(InputStream ins) {
		try {
			TarArchiveInputStream tarInput = new TarArchiveInputStream(ins);
			TarArchiveEntry currentEntry;
			while ((currentEntry = tarInput.getNextTarEntry()) != null) {
				Lang lang = RDFLanguages.filenameToLang(currentEntry.getName());
				if(lang != null) {
					System.err.println("Tar:" + currentEntry.getName());
					issuer(new StringReader(CharStreams.toString(new InputStreamReader(tarInput))), lang);
				}
			}
			tarInput.close();
		} catch (IOException e) {
			System.err.println("Something wrong in processing a tar file:" + e.getMessage());
		}
	}

	private static void processRecursively(File file) throws FileNotFoundException, IOException {
		File[] fileList = file.listFiles();
		for (File f: fileList){
			if (f.isFile()) {
				if(f.getName().endsWith(".taz")) {
					procTar(new GzipCompressorInputStream(new FileInputStream(f.getPath())));
				} else {
					dispatch(f.getPath());
				}
			} else if (f.isDirectory()) {
				if(f.getName().startsWith("."))
					continue;
				if (recursive)
					processRecursively(f);
			}
		}
	}
	
	public static void main(String[] args) {

		int idx = 0;
		recursive = false;
		checking = false;
		while (idx < args.length && args[idx].startsWith("-")) {
			if(args[idx].equals("-r")) {
				recursive = true;
			} else if(args[idx].equals("-c")) {
				checking = true;
			}
			idx++;
		}
		if(idx == args.length){
			System.out.println("Please specify the filename to be converted.");
			return;
		}
		File file = new File(args[idx]);
		if(!file.exists() || !file.canRead()){
			System.out.println("Can't read " + file);
			return;
		}
		try {
			if(file.isFile()){
				if( file.getName().endsWith(".taz") ) {
					procTar(new GzipCompressorInputStream(new FileInputStream(args[idx])));
				} else {
					dispatch(args[idx]);
				}
			} else if(file.isDirectory()){
				processRecursively(file);
			}
		} catch (FileNotFoundException e) {
			System.err.println("File not found:" + e.getMessage());
		} catch (IOException e) {
			System.err.println("IO Exception:" + e.getMessage());
		}

	}

}