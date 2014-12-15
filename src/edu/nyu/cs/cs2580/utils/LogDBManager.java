package edu.nyu.cs.cs2580.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.tmatesoft.sqljet.core.table.*;
import org.tmatesoft.sqljet.core.*;

import edu.nyu.cs.cs2580.Bhattacharyya.Pair;

public class LogDBManager {
	private static LogDBManager instance = null;

	private static final String DB_NAME = "./data/log/log.sqlite";

	// 1. tables
	// stores query log
	private static final String TABLE_QUERY = "queryLog";
	// stores click log
	private static final String TABLE_CLICK = "clickLog";
	// store query-count of all sessions
	private static final String TABLE_Q_COUNT = "querycount";
	 // store query pair - count of all sessions
	private static final String TABLE_PAIR = "paircount";

	// 2. fields
	private static final String SESSION_FIELD = "session";
	// fields of querylog
	private static final String Q_QID_FIELD = "qid";
	private static final String Q_QUERY_FIELD = "querystr";
	private static final String Q_S_INDEX = "sessq";
	// fields of clicklog
	private static final String C_CID_FIELD = "cid";
	private static final String C_CLICK_FIELD = "clickstr";
	private static final String C_QUERY_FIELD = "querystr";
	private static final String C_DOCID_FIELD = "docid";
	private static final String C_ACTION_FIELD = "action";
	// fields of querycount
	private static final String QUERY_FIELD = "querystr";
	private static final String QUERY_COUNT = "qcount";
	// fields of paircount
	private static final String QUERY1_FIELD = "query1";
	private static final String QUERY2_FIELD = "query2";
	private static final String PAIR_COUNT = "pcount";

	File dbFile;
	SqlJetDb db;

	private LogDBManager() throws SqlJetException {
		dbFile = new File(DB_NAME);
		db = SqlJetDb.open(dbFile, true);
		//db.getOptions().setAutovacuum(false);
/*
		db.runTransaction(new ISqlJetTransaction() {
			public Object run(SqlJetDb db) throws SqlJetException {
				db.getOptions().setUserVersion(1);
				return true;
			}
		}, SqlJetTransactionMode.WRITE);
		*/
	}

	public static LogDBManager getInstance(){
		if (instance == null) {
			try {
				instance = new LogDBManager();
			} catch (SqlJetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		return instance;
	}

	public void addClickLog(String session, String query, String did,
			String action) throws SqlJetException {
		db.beginTransaction(SqlJetTransactionMode.WRITE);

		ISqlJetTable clickTable = db.getTable(TABLE_CLICK);

		Date date = new Date();
		long ts = date.getTime();
		clickTable.insert(ts, session, query, did, action);

		db.commit();
	}

	public void addQueryLog(String session, String query) {
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable queryTable = db.getTable(TABLE_QUERY);
	
			Date date = new Date();
			long ts = date.getTime();
			queryTable.insert(ts, session, query);
	
			db.commit();
		} catch (SqlJetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void addQueryLog(String session, String query, long ts) {
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
	
			ISqlJetTable queryTable = db.getTable(TABLE_QUERY);
	System.out.println(session+ " " + query + " " + ts);
			queryTable.insert(ts, session, query);
			db.commit();
		} catch (SqlJetException e) {
			e.printStackTrace();
		}
	}
	
	public void addQueryCount(String query, long count){
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable qcTable = db.getTable(TABLE_Q_COUNT);
			
			qcTable.insert(query, count);
			db.commit();
		} catch (SqlJetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void updateQueryCount(String query, long count){
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable qcTable = db.getTable(TABLE_Q_COUNT);
			ISqlJetCursor updateCursor = qcTable.open();
			do {
				if (updateCursor.getString(QUERY_FIELD).equalsIgnoreCase(query)){
					updateCursor.update(query, count);
					break;
				}
			} while (updateCursor.next());
			
			updateCursor.close();
			db.commit();
		} catch (SqlJetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void addPairCount(String q1, String q2, long count){
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable pTable = db.getTable(TABLE_PAIR);
			pTable.insert(q1, q2, count);
			pTable.insert(q2, q1, count);
			db.commit();
		} catch (SqlJetException e){
			e.printStackTrace();
		}
	}
	public void updatePairCount(String q1, String q2, long count){
		try {
			db.beginTransaction(SqlJetTransactionMode.WRITE);
			ISqlJetTable pTable = db.getTable(TABLE_PAIR);
			ISqlJetCursor updateCursor = pTable.open();
			int i = -1;
			do {
				if (updateCursor.getString(QUERY1_FIELD).equalsIgnoreCase(q1)
						&& updateCursor.getString(QUERY2_FIELD).equalsIgnoreCase(q2)){
					updateCursor.update(q1, q2, count);
					i++;
				} else if (updateCursor.getString(QUERY1_FIELD).equalsIgnoreCase(q2)
						&& updateCursor.getString(QUERY2_FIELD).equalsIgnoreCase(q1)){
					updateCursor.update(q2, q1, count);
					i++;
				}
				if (i > 0){
					break;
				}
			} while (updateCursor.next());
			
			updateCursor.close();
			db.commit();
		} catch (SqlJetException e){
			e.printStackTrace();
		}
	}

	// arg: query / click
	public ArrayList<String> getAllSessionID(String tablename)
			throws SqlJetException {
		ArrayList<String> sids = new ArrayList<String>();
		Set<String> sessids = new HashSet<String>();
		db.beginTransaction(SqlJetTransactionMode.READ_ONLY);

		ISqlJetTable table;
		if (tablename.equalsIgnoreCase("query")) {
			table = db.getTable(TABLE_QUERY);
		} else {
			table = db.getTable(TABLE_CLICK);
		}

		ISqlJetCursor cursor = table.order(table.getPrimaryKeyIndexName());
		if (!cursor.eof()) {
			do {
				sessids.add(cursor.getString(SESSION_FIELD));
			} while (cursor.next());
		}
		sids.addAll(sessids);
		return sids;
	}

	public ArrayList<Pair<Long, String>> getQueryBySid(String sessionid)
			throws SqlJetException {
		ArrayList<Pair<Long, String>> queries = new ArrayList<Pair<Long, String>>();

		db.beginTransaction(SqlJetTransactionMode.READ_ONLY);

		ISqlJetTable table = db.getTable(TABLE_QUERY);

		ISqlJetCursor cursor = table.lookup(Q_S_INDEX, sessionid);
		if (!cursor.eof()) {
			do {
				queries.add(new Pair<Long, String>(Long.parseLong(cursor.getString(Q_QID_FIELD)), cursor
						.getString(Q_QUERY_FIELD)));
			} while (cursor.next());
		}
		return queries;
	}
	
	// get all from querycount
	public void getAllQueryCount(Map<String, Long> qmap){
		try {
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			ISqlJetTable qtable = db.getTable(TABLE_Q_COUNT);
			
			ISqlJetCursor cursor = qtable.order(qtable.getPrimaryKeyIndexName());
			if (!cursor.eof()){
				do {
					long count = cursor.getInteger(QUERY_COUNT);
					String query = cursor.getString(QUERY_FIELD);
					qmap.put(query, count);
				} while (cursor.next());
			}
		} catch (SqlJetException e){
			e.printStackTrace();
		}
	}
	// get all from paircount
	public void getAllPairCount(Map<Pair<String, String>, Long> pmap){
		try {
			db.beginTransaction(SqlJetTransactionMode.READ_ONLY);
			ISqlJetTable ptable = db.getTable(TABLE_PAIR);
			
			ISqlJetCursor cursor = ptable.order(ptable.getPrimaryKeyIndexName());
			if (!cursor.eof()){
				do {
					long count = cursor.getInteger(PAIR_COUNT);
					String q1 = cursor.getString(QUERY1_FIELD);
					String q2 = cursor.getString(QUERY2_FIELD);
					pmap.put(new Pair<String, String>(q1, q2), count);
				} while (cursor.next());
			}
			
		} catch (SqlJetException e){
			e.printStackTrace();
		}
	}
	
	// select from clicklog

}
