
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

        sind = (double) _indexer.documentTermFrequency(currentWord, d.getUrl());
        double ds = d.getLength();

        double pwd = sind / ds;
        double termF = (double) this._indexer.corpusTermFrequency(currentWord);
        double totalF = this._indexer._totalTermFrequency;
        double pwc = termF / totalF;
        score = (1 - lambda) * pwd + lambda * pwc;

        // return sind;
        return Math.log(score);
    }

    private ScoredDocument runquery(Query query, DocumentIndexed d) {

        double logScore = 0.0;
        for (int i = 0; i < query._tokens.size(); i++) {
            String[] tokenArray = query._tokens.get(i).split(" ");
            for (int j = 0; j < tokenArray.length; j++) {
                String currentWord = tokenArray[j];
                logScore += calculateProbability(currentWord, d);
            }
        }
        return new ScoredDocument(d, Math.exp(logScore));
    }

    @Override
    public Vector<ScoredDocument> runQuery(Query query, int numResults) {
        Queue<ScoredDocument> rankQueue = new PriorityQueue<ScoredDocument>();
        DocumentIndexed doc = null;
        int docid = -1;
        while ((doc = (DocumentIndexed) _indexer.nextDoc(query, docid)) != null) {
            rankQueue.add(runquery(query, doc));
            if (rankQueue.size() > numResults) {
                rankQueue.poll();
            }
            docid = doc._docid;
        }

        Vector<ScoredDocument> results = new Vector<ScoredDocument>();
        ScoredDocument scoredDoc = null;
        while ((scoredDoc = rankQueue.poll()) != null) {
            results.add(scoredDoc);
        }
        Collections.sort(results, new ScoredDocumentComparator());
        return results;
    }
}
