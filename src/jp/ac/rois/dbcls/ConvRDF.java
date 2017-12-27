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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.RiotException;
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
import org.apache.jena.graph.Factory;

public class ConvRDF {

	private static void issuer(String filename){
		final int interval = 10000;
		final int buffersize = 100000;

		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>(buffersize, false, 300, 1000);
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);

		ExecutorService executor = Executors.newSingleThreadExecutor();

		Runnable parser = new Runnable() {

			@Override
			public void run() {
				RDFParser parser_object = RDFParserBuilder
				.create()
				.errorHandler(ErrorHandlerFactory.errorHandlerDetailed())
				.source(filename)
				.checking(true)
				.build();
				try{
					parser_object.parse(inputStream);
				}
				catch (RiotParseException e){
					System.err.println("Parse error [" + filename + "]: " + e.getMessage());
					return;
				}
				catch (RiotNotFoundException e){
					System.err.println("File format error [" + filename + "]: " + e.getMessage());
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

	public static void main(String[] args) {

		int idx = 0;
		if(args.length == 0){
			System.out.println("Please specify the filename to be converted.");
			return;
		} else {
			File file = new File(args[idx]);
			if(!file.exists() || !file.canRead()){
				System.out.println("Can't read " + file);
				return;
			}
			if(file.isFile()){
				issuer(args[idx]);
			}else if(file.isDirectory()){
				File[] fileList = file.listFiles();
				for (File f: fileList){
					if(f.getName().startsWith("."))
						continue;
					issuer(f.getPath());
				}
			}
		}

	}

}