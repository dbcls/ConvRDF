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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFLanguages;
import org.apache.jena.riot.RiotNotFoundException;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;

public class ConvRDF {

	public static void main(String[] args) {

		final Map<String, Lang> optmap = new HashMap<String, Lang>() {
			{put("turtle", RDFLanguages.TURTLE);}
            {put("rdfxml", RDFLanguages.RDFXML);}
        };

		final int interval = 10000;
		final int buffersize = 100000;
		String informat = "rdfxml";
		int idx = 0;
		if(args.length == 0){
			System.out.println("Please specify the filename to be converted.");
			return;
		} else {
			if(args[idx].startsWith("-i:")){
				informat = args[idx].substring(3);
				idx++;
			}
			File file = new File(args[idx]);
			if(!file.exists() || !file.canRead()){
				System.out.println("Can't read " + file);
				return;
			}
		}
		if(!optmap.containsKey(informat)){
			System.out.println("Input format is either turtle or rdfxml.");
			return;
		}
		final String filename = args[idx];
		final Lang inputformat = optmap.get(informat);

		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>(buffersize);
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);

		ExecutorService executor = Executors.newSingleThreadExecutor();

		Runnable parser = new Runnable() {

			@Override
			public void run() {
				try{
					RDFDataMgr.parse(inputStream, filename, "file:///", inputformat, null);
					/*
					if(inputformat.equals("turtle")){
						RDFDataMgr.parse(inputStream, filename, "file:///", RDFLanguages.TURTLE, null);
					} else if(inputformat.equals("rdfxml")){
						RDFDataMgr.parse(inputStream, filename, "file:///", RDFLanguages.RDFXML, null);
					}
					*/
				}
				catch (RiotNotFoundException e){
					System.err.println("File format error.");
				}
			}
		};

		executor.submit(parser);

		int i = 0;
		Graph g = Factory.createDefaultGraph();
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

		executor.shutdown();
	}

}