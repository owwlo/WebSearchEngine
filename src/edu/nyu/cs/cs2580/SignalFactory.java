
package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

import edu.nyu.cs.cs2580.QueryHandler.CgiArguments;
import edu.nyu.cs.cs2580.SearchEngine.Options;

public class SignalFactory {

    public static class qlRunner extends Ranker {
        protected qlRunner(Options options, CgiArguments arguments, Indexer indexer) {
            super(options, arguments, indexer);
        }

        private static final double lamda = 0.5;

        private Map<String, Integer> makeUnigram(Vector<String> dv) {
            Map<String, Integer> unigramDic = new HashMap<String, Integer>();
            for (int i = 0; i < dv.size(); i++) {
                String word = dv.get(i);
                if (unigramDic.containsKey(word) == false) {
                    unigramDic.put(word, 1);
                }
                else {
                    unigramDic.put(word, unigramDic.get(word) + 1);
                }
            }
            return unigramDic;
        }

        public Vector<ScoredDocument> runQuery(Query query, int numResults) {
            Vector<ScoredDocument> retrieval_results = new Vector<ScoredDocument>();
            for (int i = 0; i < this._indexer.numDocs(); ++i) {
                retrieval_results.add(runquery(query._query, i));
            }
            return retrieval_results;
        }

        private double calculateProbability(String currentWord, Map<String, Integer> documentDic,
                int documentSize) {
            double score = 0;
            double sind = 0;
            // string in document
            if (documentDic.containsKey(currentWord) == true) {
                sind = (double) documentDic.get(currentWord);
            }
            // document size
            double ds = (double) documentSize;
            // probability within Document

            double pwd = sind / ds;
            // probability within corpus
            double termF = (double) this._indexer.corpusTermFrequency(currentWord);
            double totalF = (double) this._indexer.totalTermFrequency();
            double pwc = termF / totalF;
            score = (1 - lamda) * pwd + lamda * pwc;
            return Math.log(score);
        }

        public ScoredDocument runquery(String query, int did) {
            DocumentFull d = new DocumentFull(did, (IndexerFullScan) this._indexer);
            Vector<String> word_vector = d.getConvertedBodyTokens();
            Map<String, Integer> documentDic = makeUnigram(word_vector);

            Scanner s = new Scanner(query);
            Vector<String> qv = new Vector<String>();
            while (s.hasNext()) {
                String term = s.next();
                qv.add(term);
            }
            // Calculate the total
            int documentSize = word_vector.size();
            // Currently it is written as log format
            double logScore = 0.0;
            for (int i = 0; i < qv.size(); i++) {
                String currentWord = qv.get(i);
                logScore += calculateProbability(currentWord, documentDic, documentSize);
            }

            // System.out.println("log score is:"+logScore);
            return new ScoredDocument(d, Math.exp(logScore));
        }

    }

    public static class numViewRunner extends Ranker {

        protected numViewRunner(Options options, CgiArguments arguments, Indexer indexer) {
            super(options, arguments, indexer);
        }

        public ScoredDocument runquery(String query, int did) {
            DocumentFull d = new DocumentFull(did, (IndexerFullScan) this._indexer);
            int numviews = d.getNumViews();
            double score = Math.log(numviews + 1);
            return new ScoredDocument(d, score);
        }

        public Vector<ScoredDocument> runQuery(Query query, int numResults) {
            Vector<ScoredDocument> retrieval_results = new Vector<ScoredDocument>();
            for (int i = 0; i < this._indexer.numDocs(); ++i) {
                retrieval_results.add(runquery(query._query, i));
            }
            return retrieval_results;
        }
    }

    public static class phraseRunner extends Ranker {

        protected phraseRunner(Options options, CgiArguments arguments, Indexer indexer) {
            super(options, arguments, indexer);
        }

        private Map<String, Integer> makePhrase(Vector<String> dv) {
            Map<String, Integer> phraseDic = new HashMap<String, Integer>();
            for (int i = 0; i < dv.size() - 1; i++) {
                String phrase = dv.get(i) + dv.get(i + 1);
                if (phraseDic.containsKey(phrase) == false) {
                    phraseDic.put(phrase, 1);
                }
                else {
                    phraseDic.put(phrase, phraseDic.get(phrase) + 1);
                }
            }
            return phraseDic;
        }

        private Map<String, Integer> makeUnigram(Vector<String> dv) {
            Map<String, Integer> unigramDic = new HashMap<String, Integer>();
            for (int i = 0; i < dv.size(); i++) {
                String word = dv.get(i);
                if (unigramDic.containsKey(word) == false) {
                    unigramDic.put(word, 1);
                }
                else {
                    unigramDic.put(word, unigramDic.get(word) + 1);
                }
            }
            return unigramDic;
        }

        public ScoredDocument runquery(String query, int did) {
            DocumentFull d = new DocumentFull(did, (IndexerFullScan) this._indexer);
            Vector<String> dv = d.getConvertedBodyTokens();
            Map<String, Integer> dMap;
            Map<String, Integer> qMap;
            Scanner s = new Scanner(query);
            Vector<String> qv = new Vector<String>();
            while (s.hasNext()) {
                String term = s.next();
                qv.add(term);
            }
            if (qv.size() > 1) {
                dMap = makePhrase(dv);
                qMap = makePhrase(qv);
            } else {
                dMap = makeUnigram(dv);
                qMap = makeUnigram(qv);
            }
            double score = 0;
            for (String key : qMap.keySet()) {
                if (dMap.containsKey(key) == true) {
                    score = score + dMap.get(key);
                }
            }
            return new ScoredDocument(d, score);
        }

        public Vector<ScoredDocument> runQuery(Query query, int numResults) {
            Vector<ScoredDocument> retrieval_results = new Vector<ScoredDocument>();
            for (int i = 0; i < this._indexer.numDocs(); ++i) {
                retrieval_results.add(runquery(query._query, i));
            }
            return retrieval_results;
        }
    }

    public static class linearRunner extends Ranker {
        private static double beta1 = 1.0;
        private static double beta2 = 10.0;
        private static double beta3 = 0.001;
        private static double beta4 = 0.01;
        private CosineRunner cosinerunner;
        private phraseRunner phraserunner;
        private numViewRunner numviewrunner;
        private qlRunner qlrunner;

        public linearRunner(Options options, CgiArguments arguments, Indexer indexer) {
            super(options, arguments, indexer);
            cosinerunner = new CosineRunner(options, arguments, this._indexer);
            phraserunner = new phraseRunner(options, arguments, this._indexer);
            numviewrunner = new numViewRunner(options, arguments, this._indexer);
            qlrunner = new qlRunner(options, arguments, this._indexer);
        }

        public ScoredDocument runquery(String query, int did) {
            DocumentFull d = new DocumentFull(did, (IndexerFullScan) this._indexer);
            ScoredDocument co = cosinerunner.runquery(query, did);
            ScoredDocument phrase = phraserunner.runquery(query, did);
            ScoredDocument view = numviewrunner.runquery(query, did);
            ScoredDocument ql = qlrunner.runquery(query, did);
            double score = 0;

            score += co._score * beta1;
            score += phrase._score * beta3;
            score += view._score * beta4;
            score += ql._score * beta2;
            return new ScoredDocument(d, score);
        }

        public Vector<ScoredDocument> runQuery(Query query, int numResults) {
            Vector<ScoredDocument> retrieval_results = new Vector<ScoredDocument>();
            for (int i = 0; i < this._indexer.numDocs(); ++i) {
                retrieval_results.add(runquery(query._query, i));
            }
            return retrieval_results;
        }
    }

    public static class CosineRunner extends Ranker {
        protected CosineRunner(Options options, CgiArguments arguments, Indexer indexer) {
            super(options, arguments, indexer);
            n = this._indexer.numDocs() + 1;
        }

        private int n;

        public Vector<ScoredDocument> runQuery(Query query, int numResults) {
            Vector<ScoredDocument> retrieval_results = new Vector<ScoredDocument>();
            for (int i = 0; i < this._indexer.numDocs(); ++i) {
                retrieval_results.add(runquery(query._query, i));
            }
            return retrieval_results;
        }

        private Map<String, Integer> tfDic(Vector<String> dv) {
            Map<String, Integer> tfMap = new HashMap<String, Integer>();
            for (int i = 0; i < dv.size(); i++) {
                String current = dv.get(i);
                if (tfMap.containsKey(current) != true) {
                    tfMap.put(current, 1);
                }
                else {
                    tfMap.put(current, tfMap.get(current) + 1);
                }
            }
            return tfMap;
        }

        private Map<String, Integer> dfDic(Vector<String> dv) {
            Map<String, Integer> dfMap = new HashMap<String, Integer>();
            for (int i = 0; i < dv.size(); i++) {
                String current = dv.get(i);
                if (dfMap.containsKey(current) != true) {
                    dfMap.put(current, this._indexer.corpusDocFrequencyByTerm(current));
                }
            }
            return dfMap;
        }

        private void updateDf(Map<String, Integer> dfDicforD, Vector<String> qv) {
            for (int i = 0; i < qv.size(); i++) {
                String current = qv.get(i);
                if (dfDicforD.containsKey(current) == false) {
                    dfDicforD.put(current, 1);
                    // System.out.println("this is one");
                }
                else {
                    dfDicforD.put(current, dfDicforD.get(current) + 1);
                    // System.out.println("this is "+dfDicforD.get(current));
                }
            }
        }

        private double calculateNormalization(Map<String, Integer> tfMap, Map<String, Integer> dfMap) {
            double score = 0.0;
            for (String key : tfMap.keySet()) {
                double currentTf = (double) tfMap.get(key);
                // System.out.println("currentTf is:"+currentTf);
                // Here simply use e as the base
                double inverseNum = (double) n / dfMap.get(key);
                // System.out.println("inverseNum is: "+dfMap.get(key));
                double currentIdf = Math.log(inverseNum) + 1;
                double tfIdf = currentTf * currentIdf;
                score += tfIdf * tfIdf;
            }

            return Math.sqrt(score);
        }

        public ScoredDocument runquery(String query, int did) {

            DocumentFull d = new DocumentFull(did, (IndexerFullScan) this._indexer);
            Vector<String> dv = d.getConvertedBodyTokens();
            Map<String, Integer> tfDicForD = tfDic(dv);
            Map<String, Integer> dfDicForD = dfDic(dv);
            Scanner s = new Scanner(query);
            Vector<String> qv = new Vector<String>();
            while (s.hasNext()) {
                String term = s.next();
                qv.add(term);
            }
            Map<String, Integer> tfDic = tfDic(qv);
            // Take the query into account as another document
            updateDf(dfDicForD, qv);
            // calculate the normalization factor
            double normalQ = calculateNormalization(tfDic, dfDicForD);
            double normalD = calculateNormalization(tfDicForD, dfDicForD);
            double score = 0;
            // Calculating the final score
            for (String key : tfDic.keySet()) {

                if (tfDicForD.containsKey(key) == true) {
                    double tfForQ = tfDic.get(key);
                    double tfForD = tfDicForD.get(key);
                    double idf = Math.log((double) n / dfDicForD.get(key)) + 1;
                    score += tfForQ * tfForD * idf * idf;
                }

            }
            // System.out.println("score is:"+score);
            // System.out.println("normalQ is:"+normalQ);
            // System.out.println("normal D is :"+normalD);
            score = score / (normalQ * normalD);

            ScoredDocument result = new ScoredDocument(d, score);
            return result;
        }

    }
}
