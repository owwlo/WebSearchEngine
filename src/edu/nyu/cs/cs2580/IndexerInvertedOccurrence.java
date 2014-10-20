package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer {
    private DB documentDB = null;

    private ArrayList<DB> ivtIndexDBList = new ArrayList<DB>();
    private ArrayList<Map<String, List<Integer>>> ivtIndexMapList = new ArrayList<Map<String, List<Integer>>>();

    private Map<Integer, DocumentIndexed> docMap = null;
    private Map<String, Integer> docUrlMap = null;

    // Table name for index of documents.
    private static final String DOC_IDX_TBL = "docDB";
    private static final String DOC_IVT_TBL = "docIvtDB";
    private static final String DOC_URL_TBL = "docUrlDB";

    public IndexerInvertedOccurrence(Options options) {
	super(options);
	System.out.println("Using Indexer: " + this.getClass().getSimpleName());
    }

    private List<File> getAllFiles(final File folder) {
	List<File> fileList = new LinkedList<File>();

	for (final File fileEntry : folder.listFiles()) {
	    if (fileEntry.isDirectory()) {
		fileList.addAll(getAllFiles(fileEntry));
	    } else {
		fileList.add(fileEntry);
	    }
	}
	return fileList;
    }

    private class InvertIndexBuildingTask implements Runnable {
	private List<File> files;
	private int startFileIdx;
	private int endFileIdx;
	private Map<String, List<Integer>> ivtMap;

	public InvertIndexBuildingTask(List<File> files, int startFileIdx,
		int endFileIdx, Map<String, List<Integer>> ivtMap) {
	    this.files = files;
	    this.startFileIdx = startFileIdx;
	    this.endFileIdx = endFileIdx;
	    this.ivtMap = ivtMap;
	}

	@Override
	public void run() {
	    System.out.println("Thread " + Thread.currentThread().getName()
		    + " processes files from " + startFileIdx + " to "
		    + endFileIdx);
	    for (int docId = startFileIdx; docId < endFileIdx; docId++) {
		File file = files.get(docId);
		Map<String, ArrayList<Integer>> ivtMapItem = new HashMap<String, ArrayList<Integer>>();

		String htmlStr = null;
		try {
		    htmlStr = FileUtils.readFileToString(file);
		} catch (IOException e) {
		    continue;
		}
		org.jsoup.nodes.Document doc = Jsoup.parse(htmlStr);

		String title = doc.title();
		String text = doc.text();

		Stemmer s = new Stemmer();
		Scanner scanner = new Scanner(text);
		int passageLength = 0;
		while (scanner.hasNext()) {
		    String token = scanner.next().toLowerCase();
		    s.add(token.toCharArray(), token.length());
		    s.stem();

		    // Build inverted map.
		    token = s.toString();
		    if (!ivtMapItem.containsKey(token)) {
			ArrayList<Integer> occList = new ArrayList<Integer>();
			ivtMapItem.put(token, occList);
		    }
		    ArrayList<Integer> occList = ivtMapItem.get(token);
		    occList.add(passageLength);
		    ivtMapItem.put(token, occList);
		    passageLength++;
		}

		String url = null;
		try {
		    url = file.getCanonicalPath();
		} catch (IOException e) {
		    continue;
		}

		DocumentIndexed di = new DocumentIndexed(docId);
		di.setTitle(title);
		di.setUrl(url);
		di.setLength(passageLength);

		for (String token : ivtMapItem.keySet()) {
		    if (!ivtMap.containsKey(token)) {
			ivtMap.put(token, new ArrayList<Integer>());
		    }
		    List<Integer> recordList = ivtMap.get(token);

		    ArrayList<Integer> occList = ivtMapItem.get(token);
		    // sequentially add <docid, occurrence> to the posting list.
		    for (int e : occList) {
			recordList.add(docId);
			recordList.add(e);
		    }
		}

		buildDocumentIndex(di);
	    }
	}
    }

    @Override
    public void constructIndex() throws IOException {
	String corpusFolder = _options._corpusPrefix;
	System.out.println("Construct index from: " + corpusFolder);

	long start_t = System.currentTimeMillis();

	cleanUpDirectory();

	// Get all corpus files.
	List<File> files = getAllFiles(new File(corpusFolder));

	int filesPerBatch = 1000;

	initialStore(false, files.size() / filesPerBatch);

	int threadCount = 1;

	System.out.println("Start building index with " + threadCount
		+ " threads. Elapsed: "
		+ (System.currentTimeMillis() - start_t) / 1000.0 + "s");

	for (int batchNum = 0; batchNum < files.size() / filesPerBatch; batchNum++) {
	    int fileIdStart = batchNum * filesPerBatch;
	    int fileIdEnd = (batchNum + 1) * filesPerBatch;
	    if (batchNum == (files.size() / filesPerBatch) - 1) {
		fileIdEnd = files.size();
	    }

	    System.out.println("Processing files from " + fileIdStart + " to "
		    + fileIdEnd);

	    ExecutorService threadPool = Executors
		    .newFixedThreadPool(threadCount);

	    Map<String, List<Integer>> ivtMap = new HashMap<String, List<Integer>>();

	    int totalFileCount = fileIdEnd - fileIdStart;
	    int filesPerThread = totalFileCount / threadCount;
	    for (int threadId = 0; threadId < threadCount; threadId++) {
		int startFileIdx = threadId * filesPerThread + fileIdStart;
		int endFileIdx = (threadId + 1) * filesPerThread + fileIdStart;
		if (threadId == threadCount - 1) {
		    endFileIdx = fileIdEnd;
		}
		InvertIndexBuildingTask iibt = new InvertIndexBuildingTask(
			files, startFileIdx, endFileIdx, ivtMap);
		threadPool.submit(iibt);
	    }
	    threadPool.shutdown();
	    try {
		threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }

	    System.out.println(fileIdEnd
		    + " pages have been processed. Elapsed: "
		    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

	    // Write ivtMap into storage.
	    long recordsCommit = 0;

	    System.out.println("Writing Inverted Map to disk. " + fileIdEnd
		    + " pages have been processed. Elapsed: "
		    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

	    DB db = ivtIndexDBList.get(batchNum);
	    Map<String, List<Integer>> ivtMapPerDB = ivtIndexMapList
		    .get(batchNum);

	    ivtMapPerDB.putAll(ivtMap);
	    //
	    // for (String token : ivtMap.keySet()) {
	    // if (recordsCommit % 80000 == 0 && recordsCommit != 0) {
	    // db.commit();
	    // System.out.println("Records commit size: " + recordsCommit);
	    // }
	    // //List<Integer> ivtRecordList = new
	    // Vector<Integer>(ivtMap.get(token));
	    //
	    // recordsCommit++;
	    // }
	    db.commit();
	    // db.compact();
	    // db.close();
	    System.out.println("Batch commit done. Elapsed: "
		    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");
	}

	documentDB.commit();
	documentDB.compact();
	documentDB.close();

	long end_t = System.currentTimeMillis();

	System.out.println("Construct done. Duration: " + (end_t - start_t)
		/ 1000.0 + "s");
    }

    private void initialStore(boolean readOnly) {
	initialStore(readOnly, -1);
    }

    private void initialStore(boolean readOnly, int ivtDBCount) {
	// Initialize Database
	File f = new File(_options._indexPrefix, "docIdx");

	if (readOnly) {
	    documentDB = DBMaker.newFileDB(f).mmapFileEnable().readOnly()
		    .transactionDisable().make();
	} else {
	    documentDB = DBMaker.newFileDB(f).mmapFileEnable()
		    .transactionDisable().asyncWriteEnable().make();
	}

	if (ivtDBCount > 0) {
	    for (int i = 0; i < ivtDBCount; i++) {
		File ivtDbFile = new File(_options._indexPrefix, "ivtDb" + i);
		DB db = null;
		if (readOnly) {
		    db = DBMaker.newFileDB(ivtDbFile).mmapFileEnable()
			    .readOnly().commitFileSyncDisable()
			    .transactionDisable().make();
		} else {
		    db = DBMaker.newFileDB(ivtDbFile).mmapFileEnable()
			    .transactionDisable().compressionEnable()

			    .commitFileSyncDisable().asyncWriteEnable().make();
		}
		ivtIndexDBList.add(db);
	    }
	} else {
	    for (int i = 0; i < 100; i++) {
		File ivtDbFile = new File(_options._indexPrefix, "ivtDb" + i);
		if (!ivtDbFile.exists()) {
		    break;
		}
		DB db = null;
		if (readOnly) {
		    db = DBMaker.newFileDB(ivtDbFile).mmapFileEnable()
			    .readOnly().commitFileSyncDisable()
			    .transactionDisable().cacheSize(524288).make();
		} else {
		    db = DBMaker.newFileDB(ivtDbFile).mmapFileEnable()
			    .commitFileSyncDisable().transactionDisable()

			    .asyncWriteEnable().cacheSize(524288).make();
		}
		ivtIndexDBList.add(db);
	    }
	}
	for (DB d : ivtIndexDBList) {
	    Map<String, List<Integer>> hm = d.createHashMap(DOC_IVT_TBL)
		    .makeOrGet();
	    ivtIndexMapList.add(hm);
	}

	docMap = documentDB.createHashMap(DOC_IDX_TBL).makeOrGet();
	docUrlMap = documentDB.createHashMap(DOC_URL_TBL).makeOrGet();
    }

    private void cleanUpDirectory() {
	File dir = new File(_options._indexPrefix);
	dir.mkdirs();
	for (File file : dir.listFiles()) {
	    file.delete();
	}
    }

    synchronized private void buildDocumentIndex(DocumentIndexed di) {
	docMap.put(di._docid, di);
	docUrlMap.put(di.getUrl(), di._docid);
    }

    @Override
    public void loadIndex() throws IOException, ClassNotFoundException {
	initialStore(true);
    }

    @Override
    public Document getDoc(int docid) {
	return docMap.get(docid);
    }

    /**
     * In HW2, you should be using {@link DocumentIndexed}.
     */
    @Override
    public Document nextDoc(Query query, int docid) {
	return null;
    }

    @Override
    public int corpusDocFrequencyByTerm(String term) {
	// Number of documents in which {@code term} appeared, over the full corpus. 
	
        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());

        if (!ivtContainsKey(s.toString())) {
            return 0;
        }

        // Get posting list from index.
        List<Integer> l = ivtGet(s.toString());
        int count = 0;
        int last_id = -1;
        for (int i = 0; i < l.size() - 1; i += 2){
            if ( l.get(i) != last_id){
        	last_id = l.get(i);
        	++count;
            }
        }
        
        return count;
    }

    @Override
    public int corpusTermFrequency(String term) {
	// Number of times {@code term} appeared in corpus.

	// Stem given term.
	Stemmer s = new Stemmer();
	s.add(term.toLowerCase().toCharArray(), term.length());

	if (!ivtContainsKey(s.toString())) {
	    return 0;
	}

	// Get posting list from index.
	List<Integer> l = ivtGet(s.toString());

	return l.size() / 2;
    }

    private int bsInner(final int start, final int end, final int docid,
	    final List<Integer> list) {
	if (end - start <= 1) {
	    return -1;
	}
	int chk = start / 2 + end / 2;
	if (chk % 2 == 1) {
	    return -1;
	}
	if (list.get(chk) > docid) {
	    return bsInner(start, chk, docid, list);
	} else if (list.get(chk) < docid) {
	    return bsInner(chk, end, docid, list);
	}
	while (chk -2 >= start && list.get(chk-2) == docid){
	    chk -= 2;
	}
	return chk;
    }

    private int binarySearchPostList(final int docId, final List<Integer> list) {
	return bsInner(0, list.size() - 1, docId, list);
    }

    @Override
    public int documentTermFrequency(String term, String url) {
	// Number of times {@code term} appeared in the document {@code url}
	
	// Get docid for specific url.
	int docid = docUrlMap.get(url);

	// Stem given term.
	Stemmer s = new Stemmer();
	s.add(term.toLowerCase().toCharArray(), term.length());

	if (!ivtContainsKey(s.toString())) {
	    return 0;
	}

	// Get posting list from index.
	List<Integer> l = ivtGet(s.toString());

	// Use binary search looking for docid within given posting list.
	int pos = binarySearchPostList(docid, l);

	if (pos != -1) {
	    // Return term frequency for given doc and term
	    int count=0;
	    while (pos < l.size()-1 && l.get(pos)==docid){
		++count;
		pos += 2;
	    }
	    return count;
	} else {
	    return 0;
	}
    }

    private boolean ivtContainsKey(String key) {
	for (Map<String, List<Integer>> m : ivtIndexMapList) {
	    if (m.containsKey(key)) {
		return true;
	    }
	}
	return false;
    }

    private List<Integer> ivtGet(String key) {
	List<Integer> l = new ArrayList<Integer>();
	for (Map<String, List<Integer>> m : ivtIndexMapList) {
	    if (m.containsKey(key)) {
		l.addAll(m.get(key));
	    }
	}
	return l;
    }
}
