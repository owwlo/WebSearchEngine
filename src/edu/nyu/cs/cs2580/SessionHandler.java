package edu.nyu.cs.cs2580;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Date;

import edu.nyu.cs.cs2580.Bhattacharyya.Pair;
import edu.nyu.cs.cs2580.utils.LogDBManager;

public class SessionHandler {
	// time inteval within which we consider two queries are relevant
	private static int INTEVAL = 60000;
	private static double THRESHOLD = 0.0;

	// current session id
	private String cur_sess;
	private LogDBManager dbm;

	// load once from db, keep updated information in memory, keep writing to db
	private Map<String, Long> querycount;
	private Map<Pair<String, String>, Long> paircount;

	// history queries in the current session
	private ArrayList<Pair<String, Long>> queryInSession; // query + ts

	private static SessionHandler instance = null;

	private SessionHandler() {
		cur_sess = null;

		querycount = new HashMap<String, Long>();
		paircount = new HashMap<Pair<String, String>, Long>();

		queryInSession = new ArrayList<Pair<String, Long>>();
	}

	public static SessionHandler getInstance() {
		if (instance == null) {
			instance = new SessionHandler();
		}
		return instance;
	}

	// set the current session from QueryHandler
	public void setSession(String sess) {
		if (cur_sess == null) { // load history
			cur_sess = sess;
			LoadFromDB(); // get querycount and paircount
			queryInSession.clear();
		} else if (cur_sess != sess) { // write the recent session to db
			/*
			 * if (queryInSession.size() > 0) { Write2DB(); // write querycount
			 * paircount }
			 */
			cur_sess = sess;
			queryInSession.clear();
		}
	}

	// not implemented, now using incremental write
	public void Write2DB() {
		// write querycount and paircount back to database of cur_sess
	}

	// called everytime restarting the server
	public void LoadFromDB() {
		// get querycount and paircount from database
		dbm = LogDBManager.getInstance();
		if (dbm == null) {
			System.err.println("LoadFromDB: Fail to access the db");
			return;
		}
		dbm.getAllQueryCount(querycount);
		dbm.getAllPairCount(paircount);
	}

	public List<String> queryHistory(){
		List<String> l = new ArrayList<String>();
		
		for (Pair<String, Long> e : queryInSession){
			l.add(e.first);
		}
		return l;
	}
	
	public List<String> queryAllHistory() {
		List<String> l = new ArrayList<String>();
		
		l.addAll(querycount.keySet());
		return l;
	}
	// go over all the queries, and get pmi(LLR) value for each pair,
	// in every session
	public List<String> querySearch(String query){
	//	System.out.println("===search for "+query);
		List<String> l = new ArrayList<String>();
		Set<String> candidates = new HashSet<String>();

		// -1. write to db the current query
		Date date = new Date();
		long ts = date.getTime();

		if (dbm == null) {
			System.err.println("querySearch: Fail to access the db");
			return l;
		}
		dbm.addQueryLog(cur_sess, query, ts);

		// generate the first query in a session
		if (queryInSession.size() == 0) {
			queryInSession.add(new Pair<String, Long>(query, ts));

			if (querycount.containsKey(query)) {
				long newcount = querycount.get(query) + 1;
				querycount.put(query, newcount);
				dbm.updateQueryCount(query, newcount);
			//	System.out.println("update to db "+query + ":"+newcount);
			} else {
				querycount.put(query, (long) 1);
				dbm.addQueryCount(query, 1);
			//	System.out.println("add to db "+ query + ":1");
			}
			
			return l;
		}

		// 0. get the queries relevant to current query
		ArrayList<String> rel = new ArrayList<String>();
		for (int i = queryInSession.size() - 1; i >= 0; i--) {
			Pair<String, Long> last = queryInSession.get(i);
		//	System.out.println(last.second+"-"+ ts + " " + last.first);
			if (ts - last.second < INTEVAL) {
			//	System.out.println("correlated");
				rel.add(last.first);
			} else {
			//	System.out.println("irrelevant");
				break;
			}
		}
		// 0. write to queryInSession the current query
		queryInSession.add(new Pair<String, Long>(query, ts));

		// 1. write to db querycount, paircount
		if (querycount.containsKey(query)) {
			long newcount = querycount.get(query) + 1;
			querycount.put(query, newcount);
			dbm.updateQueryCount(query, newcount);
		//	System.out.println("update to db "+query + ":"+newcount);
		} else {
			querycount.put(query, (long) 1);
			dbm.addQueryCount(query, 1);
		//	System.out.println("add to db "+ query + ":1");
		}
		for (String item : rel) {
		//	System.out.println("Working towards relevant query: "+ item);
			Pair<String, String> p1 = new Pair<String, String>(query,
					item);
			Pair<String, String> p2 = new Pair<String, String>(item,
					query);
			if (paircount.containsKey(p1)) {
				long newcount = paircount.get(p1) + 1;
				paircount.put(p1, newcount);
				paircount.put(p2, newcount);
				dbm.updatePairCount(item, query, newcount);
			//	System.out.println("update to db "+ p1+":"+p2+":"+newcount);
			} else {
				paircount.put(p1, (long) 1);
				paircount.put(p2, (long) 1);
				dbm.addPairCount(item, query, 1);
			//	System.out.println("update to db "+ item+":"+query+":1");
			}
		}

		// 2. calc the co-occurrence of the rel queries with all the other
		// queries in the session
		PriorityQueue<Pair<Pair<String, String>, Double>> pmi_queue = new PriorityQueue<Pair<Pair<String, String>, Double>>(paircount.size(),new CompareByValue());
		for (String item : rel){
			for (String other : querycount.keySet()){
				if (item.equalsIgnoreCase(other)){
					continue;
				}
				Pair<String, String> pair = new Pair<String, String>(item, other);
				double pmi = 0.0;
				if (paircount.containsKey(pair)){
					double fpair = (double)paircount.get(pair);
					double fitem = (double)querycount.get(item);
					double fother = (double) querycount.get(other);
					pmi = Math.log(fpair/(fitem * fother)+1);
					pmi_queue.add(new Pair<Pair<String, String>, Double>(pair, pmi));
				}
			}
		}

		// 3. get the history queries with the co-occurrence (above threshold?)
		// of the highest top 5
		int count = 0;
		while (pmi_queue.size()>0 ){
			Pair<Pair<String,String>, Double> p = pmi_queue.poll();
			if (p.second > THRESHOLD){
				candidates.add(p.first.second);
				count++;
			} else {
				break;
			}
			if (count > 2){
				break;
			}
		}

		l.addAll(candidates);
		return l;
		
	}
	public List<String> querySearch(Query query) {
		return querySearch(query._query);
	}
	
    // for priority queue of pmi_queue<<q1, q2>, pmi>
    private class CompareByValue implements
            Comparator<Pair<Pair<String, String>, Double>> {
        @Override
        public int compare(Pair<Pair<String, String>, Double> lhs,
                Pair<Pair<String, String>, Double> rhs) {
            return rhs.second.compareTo(lhs.second);
        }
    }
    
    public static void main(String[] args){
    	SessionHandler handler = SessionHandler.getInstance();
    	
    	/*
    	try {
    		List<String> l;
        	// first session sim
    		handler.setSession("0001");
			Thread.sleep(1000);
    		l = handler.querySearch("nike");
			System.out.println("The rel list of nike");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(1000);
			l = handler.querySearch("adidas");
			System.out.println("The rel list of adidas");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(1000);
			l = handler.querySearch("air jordan");
			System.out.println("The rel list of air jordan");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(70000);
			l = handler.querySearch("inducing dog defication");
			System.out.println("The rel list of i..d..d..");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(1000);
			l = handler.querySearch("dog eat uncooked pasta");
			System.out.println("The rel list of d..e..u..p..");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(1000);
			l = handler.querySearch("inducing dog vomiting");
			System.out.println("The rel list of i..d..v..");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(70000);
			l = handler.querySearch("counterstrike");
			System.out.println("The rel list of counterstrike");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(1000);
			l = handler.querySearch("counter strike");
			System.out.println("The rel list of counter strike");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(1000);
	    	
	    	// second session sim
			handler.setSession("0002");
			Thread.sleep(1000);
			l = handler.querySearch("nike");
			System.out.println("The rel list of nike");
			for (int i = 0 ; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			Thread.sleep(1000);
			l = handler.querySearch("adidas");
			System.out.println("The rel list of adidas");
			for (int i = 0; i < l.size(); i++){
				System.out.println(l.get(i));
			}
			System.out.println();
			
			
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
    }

}