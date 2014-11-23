package edu.nyu.cs.cs2580;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.utils.ScoredDocumentComparator;

/**
 * @CS2580: Implement this class for HW3 based on your {@code RankerFavorite}
 * from HW2. The new Ranker should now combine both term features and the
 * document-level features including the PageRank and the NumViews. 
 */
public class RankerComprehensive extends Ranker {

  private final static double lambda = 0.5;	
  private final static double lambda_1=0.4;
  private final static double lambda_2=0.3;
  private final static double lambda_3=0.3;
  
  public RankerComprehensive(Options options,
      CgiArguments arguments, Indexer indexer) {
    super(options, arguments, indexer);
    System.out.println("Using Ranker: " + this.getClass().getSimpleName());
  }

  private double calculateProbability(String currentWord, DocumentIndexed d) {
      double score = 0;
      double sind = 0;

      sind = (double) _indexer.documentTermFrequency(currentWord, d._docid);
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
      logScore+=lambda_1*logScore+d.getPageRank()*lambda_2+Math.log((double)d.getNumViews())*lambda_3;
      return new ScoredDocument(d, logScore);
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
