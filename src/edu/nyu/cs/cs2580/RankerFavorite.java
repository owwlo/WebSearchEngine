
package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Scanner;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.utils.ScoredDocumentComparator;

/**
 * @CS2580: Implement this class for HW2 based on a refactoring of your favorite
 *          Ranker (except RankerPhrase) from HW1. The new Ranker should no
 *          longer rely on the instructors' {@link IndexerFullScan}, instead it
 *          should use one of your more efficient implementations.
 */
public class RankerFavorite extends Ranker {
    private final static double lambda = 0.5;

    public RankerFavorite(Options options,
            CgiArguments arguments, Indexer indexer) {
        super(options, arguments, indexer);
        System.out.println("Using Ranker: " + this.getClass().getSimpleName());
    }

    private double calculateProbability(String currentWord, DocumentIndexed d) {
        double score = 0;
        double sind = 0;
        // string in document
        /*
         * if (documentDic.containsKey(currentWord) == true) { sind = (double)
         * documentDic.get(currentWord); }
         */

        sind = (double) _indexer.documentTermFrequency(currentWord, d.getUrl());
        /*
         * if (d._docid == 637) { System.out.println("i am at 637");
         * System.out.println("sind of 637 is: " + sind);
         * System.out.println("the url is:"+d.getUrl()); }
         */
        /*
         * if (sind>0){ System.out.println("doc id is:"+d._docid);
         * System.out.println("sind is: "+sind); }
         */
        // document size
        // double ds = (double) documentSize;
        double ds = d.getLength();
        // probability within Document

        double pwd = sind / ds;
        // System.out.println("current words is: "+currentWord);
        // System.out.println("url is: "+d.getUrl());
        // System.out.println("current d is: "+d.getTitle());
        // System.out.println("ds is :"+ds);
        // / System.out.println("sind is:"+sind);
        // probability within corpus
        double termF = (double) this._indexer.corpusTermFrequency(currentWord);
        double totalF = this._indexer._totalTermFrequency;
        // System.out.println("termF: "+termF);
        // double totalF = (double) this._indexer.totalTermFrequency();
        double pwc = termF / totalF;
        score = (1 - lambda) * pwd + lambda * pwc;

        if (d._docid == 994) {
            System.out.println(score);
            System.out.println(sind);
            System.out.println(ds);
            System.out.println(termF);
            System.out.println(totalF);
        }

        // return sind;
        return Math.log(score);
    }

    private ScoredDocument runquery(Query query, DocumentIndexed d) {
        /*
         * DocumentFull d = new DocumentFull(did, (IndexerFullScan)
         * this._indexer); Vector<String> word_vector =
         * d.getConvertedBodyTokens(); Map<String, Integer> documentDic =
         * makeUnigram(word_vector); Scanner s = new Scanner(query);
         * Vector<String> qv = new Vector<String>(); while (s.hasNext()) {
         * String term = s.next(); qv.add(term); }
         */
        // Calculate the total
        // int documentSize = word_vector.size();
        // Currently it is written as log format
        // for (int i=0;i<query._tokens.size();i++){
        // }
        // query.processQuery();
        double logScore = 0.0;
        for (int i = 0; i < query._tokens.size(); i++) {
            String[] tokenArray = query._tokens.get(i).split(" ");
            for (int j = 0; j < tokenArray.length; j++) {

                String currentWord = tokenArray[j];
                logScore += calculateProbability(currentWord, d);
            }
        }

        // System.out.println("log score is:"+logScore);
        // return new ScoredDocument(d,logScore);
        return new ScoredDocument(d, Math.exp(logScore));
    }

    @Override
    public Vector<ScoredDocument> runQuery(Query query, int numResults) {
        Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
        DocumentIndexed doc = null;
        int docid = -1;
        int count = 0;
        while ((doc = (DocumentIndexed) _indexer.nextDoc(query, docid)) != null) {
            rankQueue.add(runquery(query, doc));
            if (rankQueue.size() > numResults) {

                rankQueue.poll();
            }
            docid = doc._docid;
            // System.out.println("docid is: "+docid);
            // System.out.println(docid);
        }
        // IndexerInvertedDoconly temp=(IndexerInvertedDoconly)_indexer;
        // Map<String,List<Integer>> map=temp.docInvertedMap;
        // List<Integer> current=map.get(query._query);
        // for (int i=0;i<current.size();i++)
        // System.out.printf(" "+current.get(i)+" ");

        Vector<ScoredDocument> results = new Vector<ScoredDocument>();
        ScoredDocument scoredDoc = null;
        while ((scoredDoc = rankQueue.poll()) != null) {
            results.add(scoredDoc);
        }
        Collections.sort(results, new ScoredDocumentComparator());
        return results;
    }
}
