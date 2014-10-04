package edu.nyu.cs.cs2580;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.Vector;
import java.util.HashMap;
import java.util.Scanner;
import java.util.ArrayList;

class Evaluator {
	
	public static class Metrics{
		public Metrics(int truth){
			M = (double) truth;
			K = 0.0;
			RR = 0; // accumulative for precision and recall --> fmeasure
			prGraph = new ArrayList<Double>(); // 11 elements for precision at recall
			avgPrecision = 0.0; 
			reciprocalRank = 0.0;
			dcg = 0.0; // accumulative for NDCG

		}
		public double precision1;
		public double precision5;
		public double precision10;
		public double recall1;
		public double recall5;
		public double recall10;
		public double f1;
		public double f5;
		public double f10;
		public ArrayList<Double> prGraph; // used for precision at recall points
		public double avgPrecision;
		public double dcg;
		public double ndcg1;
		public double ndcg5;
		public double ndcg10;
		public double reciprocalRank;
		
		public int RR; // relevance count in the current results
//		public int N; // total number of the results
		public double M; // total number of the relevant docs
		public double K; // current position of the results
	}

  public static void main(String[] args) throws IOException {
    HashMap < String , HashMap < Integer , Double > > relevance_judgments =
      new HashMap < String , HashMap < Integer , Double > >();
    if (args.length < 1){
      System.out.println("need to provide relevance_judgments");
      return;
    }
    String p = args[0];
    // first read the relevance judgments into the HashMap
    readRelevanceJudgments(p,relevance_judgments);
    // now evaluate the results from stdin
    evaluateStdin(relevance_judgments);
  }

  public static void readRelevanceJudgments(
    String p,HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    try {
      BufferedReader reader = new BufferedReader(new FileReader(p));
      try {
        String line = null;
        while ((line = reader.readLine()) != null){
          // parse the query,did,relevance line
          Scanner s = new Scanner(line).useDelimiter("\t");
          String query = s.next();
          int did = Integer.parseInt(s.next());
          String grade = s.next();
          double rel = 0.0;
          // convert to binary relevance
          if ((grade.equals("Perfect")) ||
            (grade.equals("Excellent")) ||
            (grade.equals("Good"))){
            rel = 1.0;
          }
          if (relevance_judgments.containsKey(query) == false){
            HashMap < Integer , Double > qr = new HashMap < Integer , Double >();
            relevance_judgments.put(query,qr);
          }
          HashMap < Integer , Double > qr = relevance_judgments.get(query);
          qr.put(did,rel);
        }
      } finally {
        reader.close();
      }
    } catch (IOException ioe){
      System.err.println("Oops " + ioe.getMessage());
    }
  }

  public static void evaluateStdin(
    HashMap < String , HashMap < Integer , Double > > relevance_judgments){
    // only consider one query per call    
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
      
      HashMap < String, Metrics > results = new HashMap < String, Metrics >();
      
      String line = null;
//      double RR = 0.0;
      double N = 0.0;
      while ((line = reader.readLine()) != null){
      	++N;
      	
        Scanner s = new Scanner(line).useDelimiter("\t");
        String query = s.next();
        int did = Integer.parseInt(s.next());
      	String title = s.next();
      	double rel = Double.parseDouble(s.next());
      	if (relevance_judgments.containsKey(query) == false){
      	  throw new IOException("query not found");
      	}

      	// get the judgments and the relevance count
      	HashMap < Integer , Double > qr = relevance_judgments.get(query); // ground truth
      	int M = 0; // number of relevant docs
      	for (double val : qr.values()){
      		if (val == 1.0){
      			++M;
      		}     		
      	}
      	
      	// check if theres an entry in the result metrics or create new metrics
      	if (results.containsKey(query) == false){
      		Metrics res = new Metrics (M);
      		results.put(query, res);
      	}
      	
      	// increment the position of the output
      	Metrics res = results.get(query);
      	res.K = N;
      	
      	if (qr.containsKey(did) != false){
      	  res.RR += qr.get(did);
      	  
      	  if (qr.get(did) == 1.0){ // for relevant output
      	  	// avg precision
      	  	res.avgPrecision += res.RR/res.K;
      	  	
        	  // reciprocal rank
        	  if (res.reciprocalRank == 0.0){
        	  	res.reciprocalRank = 1/N;
        	  }
        	  
        	  // dcg
        	  res.dcg += Math.log(2)/Math.log(N+1);
      	  }
      	}
      	// precision-recall graph
      	double precision = (double)res.RR / res.K;
      	double recall = (double) res.RR / res.M;
      	double fmeasure = 1.0/(0.5*(1.0/precision)+0.5*(1.0/recall));
      	int length = res.prGraph.size();
      	if ( length < 11 && recall*10 >= length ){ //&& recall*10 < length+1
      		res.prGraph.add(length, precision);
      	}
 
      	// calculate idcg to normalize
  			double idcg = 0.0; 
  			double count = Math.min(res.M, N);
  			for (int i = 1; i <= count ; ++i){
  				idcg += Math.log(2)/Math.log(i+1);
  			}
      	
      	// precision recall fmeasure NDCG at 3 points
      	if (N == 1){
      		res.precision1 = precision;
      		res.recall1 = recall;
      		res.f1 = fmeasure;
      		res.ndcg1 = res.dcg/idcg;
      	} else if (N == 5){
      		res.precision5 = precision;
      		res.recall5 = recall;
      		res.f5 = fmeasure;
      		res.ndcg5 = res.dcg/idcg;
      	} else if (N == 10){
      		res.precision10 = precision;
      		res.recall10 = recall;
      		res.f10 = fmeasure;
      		res.ndcg10 = res.dcg/idcg;
      	}
      	
      }
      // output the evaluation
      for (String key : results.keySet()){
      	Metrics value = results.get(key);
      	System.out.print(key + "\t" + value.precision1
      			+ "\t" + value.precision5
      			+ "\t" + value.precision10
      			+ "\t" + value.recall1
      			+ "\t" + value.recall5
      			+ "\t" + value.recall10
      			+ "\t" + value.f1
      			+ "\t" + value.f5
      			+ "\t" + value.f10);
      	for (double p : value.prGraph){
      		System.out.print("\t" + p);
      	}
      	System.out.print("\t" + value.avgPrecision
      			+ "\t" + value.ndcg1
      			+ "\t" + value.ndcg5
      			+ "\t" + value.ndcg10
      			+ "\t" + value.reciprocalRank);   	
      }
      
    } catch (Exception e){
      System.err.println("Error:" + e.getMessage());
    }
  }
}
