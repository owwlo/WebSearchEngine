package edu.nyu.cs.cs2580;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class LogMinerNumviews extends LogMiner {

  public LogMinerNumviews(Options options) {
    super(options);
  }

  /**
   * This function processes the logs within the log directory as specified by
   * the {@link _options}. The logs are obtained from Wikipedia dumps and have
   * the following format per line: [language]<space>[article]<space>[#views].
   * Those view information are to be extracted for documents in our corpus and
   * stored somewhere to be used during indexing.
   *
   * Note that the log contains view information for all articles in Wikipedia
   * and it is necessary to locate the information about articles within our
   * corpus.
   *
   * @throws IOException
   */
  @Override
	public void compute() throws IOException {
	  System.out.println("Computing using " + this.getClass().getName());
		/*
		 * First step, Iterate all the documents and store store-int
		 * relationship During this step, deal with the redirection as well.
		 */
		File dir = new File(_options._corpusPrefix);
		String[] documents = dir.list();
		Map<String, Integer> indexDocs = new HashMap<String, Integer>();
		for (String document : documents) {
			if (document.startsWith(".")==true)
	    		 continue;
			indexDocs.put(document, 0);
		}
		File logDir = new File(_options._logPrefix);
		documents = logDir.list();
		for (String document : documents) {
			FileInputStream fis = new FileInputStream(_options._logPrefix + "/"
					+ document);

			// Construct BufferedReader from InputStreamReader
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] seperates = line.split(" ");
				if (seperates.length < 3
						|| seperates[2].matches("\\d+") == false||indexDocs.containsKey(seperates[1])==false)
					continue;
				int numView = Integer.parseInt(seperates[2]);
				if (indexDocs.containsKey(seperates[1]) == true
						&& indexDocs.containsKey(seperates[1] + ".html") == true) {
					indexDocs.put(seperates[1] + ".html",
							indexDocs.get(seperates[1] + ".html") + numView);
				} else {
					indexDocs.put(seperates[1], indexDocs.get(seperates[1])
							+ numView);
				}
			}

			br.close();
		}
		File indexDic=new File(_options._indexPrefix);
		if (indexDic.exists()==false)
			indexDic.mkdir();
		//Write to files
		String des=_options._indexPrefix+"/../numView.index";
		PrintWriter writer = new PrintWriter(des,"UTF-8");
		for (String key:indexDocs.keySet()){
			
			int numView=indexDocs.get(key);
			writer.println(key+" "+numView);
		}
		writer.close();
		indexDocs.clear();
	  
        return;
  }

  /**
   * During indexing mode, this function loads the NumViews values computed
   * during mining mode to be used by the indexer.
   * 
   * @throws IOException
   */
  @Override
	public Object load() throws IOException {
		System.out.println("Loading using " + this.getClass().getName());
		FileInputStream fis = new FileInputStream(_options._indexPrefix
				+ "/../numView.index");
		BufferedReader br = new BufferedReader(new InputStreamReader(fis));
		String line = null;
		Map<String, Double> numView = new HashMap<String, Double>();
		while ((line = br.readLine()) != null) {
			String[] splits = line.split(" ");
			double val = Double.valueOf(splits[1]);
			numView.put(splits[0], val);
		}
		br.close();
		return numView;
	}
}
