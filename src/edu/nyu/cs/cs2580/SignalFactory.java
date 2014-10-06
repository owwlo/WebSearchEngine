
package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

public class SignalFactory {
    public static SignalRunner makeSignalRunner(String signal, Index _index) {
        if (signal.equalsIgnoreCase("cosine") == true) {
            return new CosineRunner(_index);
        }
        else {
        	if (signal.equalsIgnoreCase("numviews")==true){
        		return new numViewRunner(_index);
        	}
            if (signal.equalsIgnoreCase("phrase") == true) {

                return new phraseRunner(_index);
            }

            if (signal.equalsIgnoreCase("linear") == true) {
                return new linearRunner(_index);
            }

            if (signal.equalsIgnoreCase("QL") == true) {
                // System.out.println("hi this is QL");
                return new qlRunner(_index);
            }

        }
        return new CosineRunner(_index);
    }
}

interface SignalRunner {
    public ScoredDocument runquery(String query, int did);
}

class qlRunner implements SignalRunner {
    private final Index _index;
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
        double termF = (double) _index.termFrequency(currentWord);
        double totalF = (double) _index.termFrequency();
        double pwc = termF / totalF;
        score = (1 - lamda) * pwd + lamda * pwc;
        return Math.log(score);
    }

    @Override
    public ScoredDocument runquery(String query, int did) {

        Document d = _index.getDoc(did);
        Vector<String> word_vector = d.get_body_vector();
        Map<String, Integer> documentDic = makeUnigram(word_vector);
        int totalNum = 0;

        for (String key : documentDic.keySet()) {
            totalNum += documentDic.get(key);
        }
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
        return new ScoredDocument(did, d.get_title_string(), Math.exp(logScore));
    }

    public qlRunner(Index _index) {
        this._index = _index;

    }
}

class numViewRunner implements SignalRunner {
    private Index _index;

    public numViewRunner(Index _index) {
        this._index = _index;
    }

    @Override
    public ScoredDocument runquery(String query, int did) {
        Document d = _index.getDoc(did);
        int numviews = d.get_numviews();
        double score = Math.log(numviews);
        return new ScoredDocument(did, d.get_title_string(), score);
    }
}

class phraseRunner implements SignalRunner {
    private Index _index;

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

    @Override
    public ScoredDocument runquery(String query, int did) {
        Document d = _index.getDoc(did);
        Vector<String> dv = d.get_body_vector();
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
                score = score + 1;
            }
        }
        return new ScoredDocument(did, d.get_title_string(), score);
    }

    public phraseRunner(Index _index) {
        this._index = _index;
    }
}
class linearRunner implements SignalRunner{
	private static double beta1=1.0;
	private static double beta2=10.0;
	private static double beta3=0.05;
	private static double beta4=0.05;
	private Index _index;
	private CosineRunner cosinerunner;
	private phraseRunner phraserunner;
	private numViewRunner numviewrunner;
	private qlRunner qlrunner;
	public linearRunner(Index _index) {
	        this._index = _index;
	        cosinerunner = new CosineRunner(_index);
	        phraserunner = new phraseRunner(_index);
	        numviewrunner = new numViewRunner(_index);
	        qlrunner = new qlRunner(_index);
	}
	 @Override
	 public ScoredDocument runquery(String query, int did) {
		  Document d = _index.getDoc(did);
          ScoredDocument co = cosinerunner.runquery(query, did);
          ScoredDocument phrase = phraserunner.runquery(query, did);
          ScoredDocument view = numviewrunner.runquery(query, did);
          ScoredDocument ql = qlrunner.runquery(query, did);
          double score=0;
          if (d.get_title_string().equals("google")){
        	 // System.out.println(co._score);
        	 // System.out.println(phrase._score);
        	 // System.out.println(view._score);
        	 // System.out.println(Math.exp(ql._score));
          }
          score+=co._score*beta1;
          score+=phrase._score*beta3;
          score+=view._score*beta4;
          score+=ql._score*beta2;
          return new ScoredDocument(did, d.get_title_string(), score);
	 }
	
}
class CosineRunner implements SignalRunner {
    private Index _index;
    private int n;

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
                dfMap.put(current, _index.documentFrequency(current));
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

    private double calculateNormalization2(Map<String, Integer> tfMap, Map<String, Integer> dfMap) {
        double score = 0.0;
        for (String key : tfMap.keySet()) {
            System.out.println("key is: " + key);
            double currentTf = (double) tfMap.get(key);
            System.out.println("currentTf is:" + currentTf);
            // Here simply use e as the base
            double inverseNum = (double) n / dfMap.get(key);
            System.out.println("inverseNum is: " + inverseNum);
            double currentIdf = Math.log(inverseNum) + 1;
            double tfIdf = currentTf * currentIdf;
            score += tfIdf * tfIdf;
        }
        
        return Math.sqrt(score);
    }

    @Override
    public ScoredDocument runquery(String query, int did) {

        Document d = _index.getDoc(did);
        Vector<String> dv = d.get_body_vector();
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

        ScoredDocument result = new ScoredDocument(did, d.get_title_string(), score);
        return result;
    }

    public CosineRunner(Index _index) {
        this._index = _index;
        n = _index.numDocs() + 1;
    }
}
