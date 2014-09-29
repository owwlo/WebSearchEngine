package edu.nyu.cs.cs2580;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;

public class SignalFactory {
   public static SignalRunner makeSignalRunner(String signal,Index _index) {
	   if (signal.equals("cosine")==true){
		   return new CosineRunner(_index);
	   }
	   else{
		   if (signal.equals("phrase")==true){
			   
			   return new phraseRunner(_index);
		   }
		   
		   if (signal.equals("numviews")==true){
			   return new numViewRunner(_index);
		   }
		   
	   }
	   return new CosineRunner(_index);
   }
}

interface SignalRunner{
	 public ScoredDocument runquery(String query, int did);
}

class numViewRunner implements SignalRunner{
	private Index _index;
	public numViewRunner(Index _index){
		this._index=_index;
	}
	
	@Override
	public ScoredDocument runquery(String query, int did){
		Document d =_index.getDoc(did);
		double score=0;
		score=(double)d.get_numviews();
		return new ScoredDocument(did, d.get_title_string(), score);
	}
	
}

class phraseRunner implements SignalRunner{
	private Index _index;
	private Map<String,Integer> makePhrase(Vector<String> dv){
		Map<String,Integer> phraseDic = new HashMap<String,Integer> ();
		for (int i=0;i<dv.size()-1;i++){
			String phrase=dv.get(i)+dv.get(i+1);
			if (phraseDic.containsKey(phrase)==false){
			  phraseDic.put(phrase, 1);	
			}
			else{
				phraseDic.put(phrase, phraseDic.get(phrase)+1);
			}
		}
		String start="_START_";
		String end="_END";
		if (dv.size()>0){
			phraseDic.put(start+dv.get(0), 1);
			phraseDic.put(dv.get(dv.size()-1)+end, 1);
		}
		else{
			phraseDic.put(start+end, 1);
		}
		return phraseDic;
	}
	
	@Override
	public ScoredDocument runquery(String query,int did){
		Document d = _index.getDoc(did);
		Vector < String > dv =d.get_body_vector();
		Map<String,Integer> dMap=makePhrase(dv);
		Scanner s = new Scanner(query);
	    Vector < String > qv = new Vector < String > ();
	    while (s.hasNext()){
	      String term = s.next();
	      qv.add(term);
	    }
	    Map<String,Integer> qMap = makePhrase(qv);
		double score=0;
		for(String key : qMap.keySet()){
			if (dMap.containsKey(key)==true){
				score=score+1;
			}
		}
		return new ScoredDocument(did, d.get_title_string(), score);
	}
	public phraseRunner(Index _index){
		this._index=_index;
	}
}

class CosineRunner implements SignalRunner{
	private Index _index;
	private int n;
	private Map<String,Integer> tfDic(Vector<String> dv){
		Map<String,Integer> tfMap=new HashMap<String,Integer>();
		for (int i=0;i<dv.size();i++){
			String current=dv.get(i);
			if (tfMap.containsKey(current)!=true){
				tfMap.put(current,1);
			}
			else{
				tfMap.put(current, tfMap.get(current)+1);
			}
		}
		return tfMap;
	}
	private Map<String,Integer> dfDic(Vector<String> dv){
		Map<String,Integer> dfMap=new HashMap<String,Integer> ();
		for (int i=0;i<dv.size();i++){
			String current=dv.get(i);
			if (dfMap.containsKey(current)!=true){
				dfMap.put(current,_index.documentFrequency(current));
			}
		}
		return dfMap;
	}
	private void updateDf(Map<String,Integer> dfDicforD, Vector<String> qv){
		for (int i=0;i<qv.size();i++){
			String current=qv.get(i);
			if (dfDicforD.containsKey(current)==false){
				dfDicforD.put(current,1);
				//System.out.println("this is one");
			}
			else{
				dfDicforD.put(current,dfDicforD.get(current)+1);
				//System.out.println("this is "+dfDicforD.get(current));
			}
		}
	}
	private double calculateNormalization(Map<String,Integer> tfMap, Map<String,Integer> dfMap){
		double score=0.0;
		for (String key: tfMap.keySet()){
			double currentTf=(double)tfMap.get(key);
			//System.out.println("currentTf is:"+currentTf);
			//Here simply use e as the base
			double inverseNum=(double)n/dfMap.get(key);
			//System.out.println("inverseNum is: "+dfMap.get(key));
			double currentIdf=Math.log(inverseNum)+1;
			double tfIdf=currentTf*currentIdf;
			score +=tfIdf*tfIdf;		    
		}
		
		return Math.sqrt(score);
	}
	private double calculateNormalization2(Map<String,Integer> tfMap, Map<String,Integer> dfMap){
		double score=0.0;
		for (String key: tfMap.keySet()){
			System.out.println("key is: "+key);
			double currentTf=(double)tfMap.get(key);
			System.out.println("currentTf is:"+currentTf);
			//Here simply use e as the base
			double inverseNum=(double)n/dfMap.get(key);
			System.out.println("inverseNum is: "+inverseNum);
			double currentIdf=Math.log(inverseNum)+1;
			double tfIdf=currentTf*currentIdf;
			score +=tfIdf*tfIdf;		    
		}
		
		return Math.sqrt(score);
	}
	@Override
	 public ScoredDocument runquery(String query, int did){
		
		Document d = _index.getDoc(did);
		Vector < String > dv =d.get_body_vector();
		Map<String,Integer> tfDicForD=tfDic(dv);
		Map<String,Integer> dfDicForD=dfDic(dv);
		Scanner s = new Scanner(query);
	    Vector < String > qv = new Vector < String > ();
	    while (s.hasNext()){
	      String term = s.next();
	      qv.add(term);
	    }
	    Map<String,Integer> tfDic = tfDic(qv);
	    //Take the query into account as another document
	    updateDf(dfDicForD,qv);
	    //calculate the normalization factor
	    double normalQ=calculateNormalization(tfDic,dfDicForD);
	    double normalD=calculateNormalization(tfDicForD,dfDicForD);
	    double score=0;
	    //Calculating the final score
	    for (String key: tfDic.keySet()){
	    	if (tfDicForD.containsKey(key)==true){
	    	   double tfForQ=tfDic.get(key);
	    	   double tfForD=tfDicForD.get(key);
	    	   double idf=Math.log((double)n/dfDicForD.get(key))+1;
	    	   score+=tfForQ*tfForD*idf*idf;
	    	}
	    	
	    }
	    //System.out.println("score is:"+score);
	    //System.out.println("normalQ is:"+normalQ);
	    //System.out.println("normal D is :"+normalD);
	    score=score/(normalQ*normalD);
	    
	    ScoredDocument result = new ScoredDocument(did, d.get_title_string(), score);
	    return result;
	}
	
	public CosineRunner(Index _index){
		this._index=_index;
		n=_index.numDocs()+1;
	}
}
