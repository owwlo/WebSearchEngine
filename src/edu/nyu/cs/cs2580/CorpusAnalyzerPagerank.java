
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW3.
 */
public class CorpusAnalyzerPagerank extends CorpusAnalyzer {
    public CorpusAnalyzerPagerank(Options options) {
        super(options);
        indexDocs = new HashMap<String, Integer>();
    }

    /**
     * This function processes the corpus as specified inside {@link _options}
     * and extracts the "internal" graph structure from the pages inside the
     * corpus. Internal means we only store links between two pages that are
     * both inside the corpus. Note that you will not be implementing a real
     * crawler. Instead, the corpus you are processing can be simply read from
     * the disk. All you need to do is reading the files one by one, parsing
     * them, extracting the links for them, and computing the graph composed of
     * all and only links that connect two pages that are both in the corpus.
     * Note that you will need to design the data structure for storing the
     * resulting graph, which will be used by the {@link compute} function.
     * Since the graph may be large, it may be necessary to store partial graphs
     * to disk before producing the final graph.
     * 
     * @throws IOException
     */

    private Map<String, Integer> indexDocs;
    private List<List<Integer>> graph;
    double lambda = 0.9;
    int priter = 2;

    @Override
    public void prepare() throws IOException {
        System.out.println("Preparing " + this.getClass().getName());
        /*
         * First step, Iterate all the documents and store store-int
         * relationship During this step, deal with the redirection as well.
         */
        File dir = new File(_options._corpusPrefix);
        String[] documents = dir.list();
        for (String document : documents) {
            if (document.startsWith(".") == true)
                continue;
            int currentIndex = indexDocs.size();
            indexDocs.put(document, currentIndex);
            if (document.endsWith(".html") == true) {
                int finishPosition = document.lastIndexOf(".html");
                String fullName = document.substring(0, finishPosition);
                if (indexDocs.containsKey(fullName) == true)
                    indexDocs.put(fullName, currentIndex);
            } else {
                String possibleRedirection = document + ".html";
                if (indexDocs.containsKey(possibleRedirection) == true)
                    indexDocs.put(document, indexDocs.get(possibleRedirection));
            }

        }
        /*
         * TESTING THE MAP...... for (String key:indexDocs.keySet()){
         * System.out.println(key+" "+indexDocs.get(key)+" "); }
         */

        /*
         * Second step, Iterate all the documents again Used a
         * List<List<Integer>> to store the graph relationship
         */
        graph = new ArrayList<List<Integer>>();
        for (int i = 0; i < indexDocs.size(); i++)
            graph.add(new ArrayList<Integer>());
        for (String document : documents) {
            if (document.startsWith(".") == true)
                continue;
            String fullPath = _options._corpusPrefix + "/" + document;
            File file = new File(fullPath);
            HeuristicLinkExtractor extractor = new HeuristicLinkExtractor(file);
            String link = null;
            int documentIndex = indexDocs.get(document);
            while (true) {
                link = extractor.getNextInCorpusLinkTarget();
                if (link == null)
                    break;
                if (indexDocs.containsKey(link) == true && documentIndex != indexDocs.get(link)) {
                    int outIndex = indexDocs.get(link);
                    List<Integer> target = graph.get(documentIndex);
                    if (target.contains(outIndex) == false)
                        target.add(outIndex);
                }
            }
        }
        /*
         * for (int i=0;i<graph.size();i++){
         * System.out.println(i+"output links are :"+graph.get(i).size());
         * System.out.println(graph.get(i)); }
         */
        return;
        /*
         * Caveat: In this case, we didn't take into account of self-link
         */
    }

    /**
     * This function computes the PageRank based on the internal graph generated
     * by the {@link prepare} function, and stores the PageRank to be used for
     * ranking. Note that you will have to store the computed PageRank with each
     * document the same way you do the indexing for HW2. I.e., the PageRank
     * information becomes part of the index and can be used for ranking in
     * serve mode. Thus, you should store the whatever is needed inside the same
     * directory as specified by _indexPrefix inside {@link _options}.
     * 
     * @throws IOException
     */
    @Override
    public void compute() throws IOException {
        System.out.println("Computing using " + this.getClass().getName());
        double[] currentPR = new double[indexDocs.size()];
        double[] nextPR = new double[indexDocs.size()];
        for (int i = 0; i < currentPR.length; i++) {
            currentPR[i] = 1.0;
            nextPR[i] = 0.0;
        }
        for (int iter = 0; iter < priter; iter++) {
            for (int i = 0; i < graph.size(); i++) {
                List<Integer> outlinks = graph.get(i);
                double outbound = (double) outlinks.size();
                for (int j = 0; j < outlinks.size(); j++)
                    nextPR[outlinks.get(j)] += currentPR[i] / outbound;
            }
            for (int i = 0; i < nextPR.length; i++) {
                currentPR[i] = lambda * nextPR[i] + (1 - lambda);
            }
            for (int i = 0; i < nextPR.length; i++) {
                nextPR[i] = 0.0;
            }
        }
        new File(_options._indexPrefix).mkdir();
        // Write to files
        String des = _options._indexPrefix + "/PR.index";
        PrintWriter writer = new PrintWriter(des, "UTF-8");
        for (String key : indexDocs.keySet()) {
            if (indexDocs.containsKey(key + ".html") == true) {
                writer.println(key + " " + 0.0);
                continue;
            }
            double prVal = currentPR[indexDocs.get(key)];
            writer.println(key + " " + prVal);
        }
        writer.close();
        indexDocs.clear();
        graph.clear();
        return;
    }

    /**
     * During indexing mode, this function loads the PageRank values computed
     * during mining mode to be used by the indexer.
     * 
     * @throws IOException
     */
    @Override
    public Object load() throws IOException {
        System.out.println("Loading using " + this.getClass().getName());
        return null;
    }
}
