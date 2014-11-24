
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jsoup.Jsoup;
import org.owwlo.InvertedIndexing.InvertedIndexBuilder;
import org.owwlo.InvertedIndexing.InvertedIndexBuilder.IvtMapInteger;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.utils.PersistentStoreManager;
import edu.nyu.cs.cs2580.utils.PersistentStoreManager.TermFrequencyManager;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer {
    private List<IvtMapInteger> ivtIndexMapList = new ArrayList<IvtMapInteger>();

    private Map<Integer, DocumentIndexed> docMap = null;
    private Map<String, Integer> docUrlMap = null;
    private Map<String, Object> infoMap = null;
    private static String previousQuery=new String();
    private static int previousDocid=-1;
    private static Vector<Vector<Integer>> cachePos=new Vector<Vector<Integer>> ();
    // Table name for index of documents.
    private static final String DOC_IDX_TBL = "docDB";
    private static final String DOC_URL_TBL = "docUrlDB";
    private static final String DOC_INFO_TBL = "docInfoDB";
    
    private TermFrequencyManager tfm;

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
        private long termCount = 0;

        public InvertIndexBuildingTask(List<File> files, int startFileIdx,
                int endFileIdx, Map<String, List<Integer>> ivtMap) {
            this.files = files;
            this.startFileIdx = startFileIdx;
            this.endFileIdx = endFileIdx;
            this.ivtMap = ivtMap;
        }

        public long getTermCount() {
            return termCount;
        }

        @Override
        public void run() {
            System.out.println("Thread " + Thread.currentThread().getName()
                    + " processes files from " + startFileIdx + " to "
                    + endFileIdx);
            for (int docId = startFileIdx; docId < endFileIdx; docId++) {
                File file = files.get(docId);
                Map<String, List<Integer>> ivtMapItem = new HashMap<String, List<Integer>>();
                Map<String, Integer> ferqMap = new HashMap<String, Integer>();

                org.jsoup.nodes.Document doc;
                try {
                    doc = Jsoup.parse(file, "UTF-8");
                } catch (IOException e1) {
                    continue;
                }

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

                    if (token.length() < 1 || token.length() > 20) {
                        continue;
                    }

                    if (!ferqMap.containsKey(token)) {
                        ferqMap.put(token, 0);
                    }
                    ferqMap.put(token, ferqMap.get(token) + 1);

                    if (!ivtMapItem.containsKey(token)) {
                        List<Integer> occList = new ArrayList<Integer>();
                        ivtMapItem.put(token, occList);
                    }
                    List<Integer> occList = ivtMapItem.get(token);
                    occList.add(passageLength);
                    ivtMapItem.put(token, occList);
                    passageLength++;
                }

                termCount += passageLength;

                tfm.addTermFrequencyForDoc(docId, ferqMap);

                String url = file.getName();

                DocumentIndexed di = new DocumentIndexed(docId);
                di.setTitle(title);
                di.setUrl(url);
                di.setLength(passageLength);

                for (String token : ivtMapItem.keySet()) {
                    if (!ivtMap.containsKey(token)) {
                        ivtMap.put(token, new ArrayList<Integer>());
                    }
                    List<Integer> recordList = ivtMap.get(token);

                    List<Integer> occList = ivtMapItem.get(token);
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

        int filesPerBatch = 1750;

        // initialStore(false, files.size() / filesPerBatch);

        int threadCount = 1;

        System.out.println("Start building index with " + threadCount
                + " threads. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

        infoMap = new HashMap<String, Object>();
        docMap = new HashMap<Integer, DocumentIndexed>();
        docUrlMap = new HashMap<String, Integer>();

        infoMap.put("_numDocs", files.size());

        InvertedIndexBuilder builder = InvertedIndexBuilder.getBuilder(new File(
                _options._indexPrefix));

        tfm = new TermFrequencyManager(_options._indexPrefix);

        long termCount = 0;

        for (int batchNum = 0; batchNum < files.size() / filesPerBatch + 1; batchNum++) {
            int fileIdStart = batchNum * filesPerBatch;
            int fileIdEnd = (batchNum + 1) * filesPerBatch;
            if (fileIdEnd > files.size()) {
                fileIdEnd = files.size();
            }

            System.out.println("Processing files from " + fileIdStart + " to "
                    + fileIdEnd);

            ExecutorService threadPool = Executors
                    .newFixedThreadPool(threadCount);

            IvtMapInteger ivtMapFile = builder.createDistributedIvtiIntegerMap();
            Map<String, List<Integer>> ivtMap = new HashMap<String, List<Integer>>();

            List<InvertIndexBuildingTask> taskList = new ArrayList<InvertIndexBuildingTask>();

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
                taskList.add(iibt);
            }
            threadPool.shutdown();
            try {
                threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Combine all posting lists for N threads.
            for (InvertIndexBuildingTask iibt : taskList) {
                termCount += iibt.getTermCount();
            }

            System.out.println("Writing Inverted Map to disk. " + fileIdEnd
                    + " pages have been processed. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

            ivtMapFile.putAll(ivtMap);
            ivtMapFile.close();

            System.out.println("Batch commit done. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");
        }

        infoMap.put("_totalTermFrequency", termCount);

        builder.close();
        tfm.close();

        CorpusAnalyzer ca = new CorpusAnalyzerPagerank(_options);
        LogMiner lm = new LogMinerNumviews(_options);
        Map<String, Double> pageRankMap = (Map<String, Double>) ca.load();
        Map<String, Double> numViewsMap = (Map<String, Double>) lm.load();

        for (Map.Entry<Integer, DocumentIndexed> die : docMap.entrySet()) {
            DocumentIndexed di = die.getValue();
            String basename = di.getUrl();
            di.setPageRank((float) (double) pageRankMap.get(basename));
            di.setNumViews((int) (double) numViewsMap.get(basename));
        }

        storeVariables();

        long end_t = System.currentTimeMillis();

        System.out.println("Construct done. Duration: " + (end_t - start_t)
                / 1000.0 + "s");
    }

    private void storeVariables() {
        File docMapFile = new File(this._options._indexPrefix, DOC_IDX_TBL);
        File docUrlFile = new File(this._options._indexPrefix, DOC_URL_TBL);
        File docInfoFile = new File(this._options._indexPrefix, DOC_INFO_TBL);
        PersistentStoreManager.writeObjectToFile(docMapFile, docMap);
        PersistentStoreManager.writeObjectToFile(docUrlFile, docUrlMap);
        PersistentStoreManager.writeObjectToFile(docInfoFile, infoMap);
    }

    private void readVariables() {
        File docMapFile = new File(this._options._indexPrefix, DOC_IDX_TBL);
        File docUrlFile = new File(this._options._indexPrefix, DOC_URL_TBL);
        File docInfoFile = new File(this._options._indexPrefix, DOC_INFO_TBL);
        docMap = (Map<Integer, DocumentIndexed>) PersistentStoreManager
                .readObjectFromFile(docMapFile);
        docUrlMap = (Map<String, Integer>) PersistentStoreManager.readObjectFromFile(docUrlFile);
        infoMap = (Map<String, Object>) PersistentStoreManager.readObjectFromFile(docInfoFile);

        _totalTermFrequency = (Long) infoMap.get("_totalTermFrequency");
        _numDocs = (Integer) infoMap.get("_numDocs");
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
        InvertedIndexBuilder builder = InvertedIndexBuilder.getBuilder(new File(
                _options._indexPrefix));

        tfm = new TermFrequencyManager(_options._indexPrefix);

        IvtMapInteger ivtMapBatch = builder.getUnifiedDistributedIvtiIntegerMap();
        ivtIndexMapList.add(ivtMapBatch);
        readVariables();
    }

    @Override
    public Document getDoc(int docid) {
        return docMap.get(docid);
    }

    /**
     * In HW2, you should be using {@link DocumentIndexed}.
     */
    private static int nextInOccurence(int docId, List<Integer> postinglist,int phraseIndex,int termIndex) {
    	int start=cachePos.get(phraseIndex).get(termIndex);
        for (int i = start; i < postinglist.size(); i += 2) {
            if (postinglist.get(i) > docId){
            	cachePos.get(phraseIndex).set(termIndex, i);
                return postinglist.get(i);
            }
        }
        return -1;
    }

    private static int nextForOccurence(int docId, Vector<List<Integer>> postinglists,int phraseIndex) {
        // System.out.println("current id is: "+docId);
        int previousVal = -1;
        boolean equilibrium = true;
        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < postinglists.size(); i++) {
            int currentId = nextInOccurence(docId, postinglists.get(i),phraseIndex,i);
            if (currentId < 0)
                return -1;
            if (previousVal < 0) {
                previousVal = currentId;
                maximum = currentId;
            }
            else {
                if (previousVal != currentId) {
                    equilibrium = false;
                    maximum = Math.max(maximum, currentId);
                }
            }
        }
        if (equilibrium == true)
            return previousVal;
        else
            return nextForOccurence(maximum - 1, postinglists,phraseIndex);
    }

    private static int nextPos(List<Integer> postinglist, int docId, int pos,int phrasePos,int termPos) {
        int docPosition = -1;
        int start=cachePos.get(phrasePos).get(termPos);
        for (int i = start; i < postinglist.size(); i += 2) {
            if (postinglist.get(i) == docId) {
                docPosition = i;
                cachePos.get(phrasePos).set(termPos, i);
                break;
            }
        }
        if (docPosition == -1)
            return -1;
        int Pos = docPosition + 1;
        while (Pos < postinglist.size() && postinglist.get(Pos - 1) == docId) {
            if (postinglist.get(Pos) > pos){
            	cachePos.get(phrasePos).set(termPos, Pos-1);
                return postinglist.get(Pos);
            }
            Pos += 2;
        }
        return -1;
    }

    private static int nextPhrase(int docId, int pos, Vector<List<Integer>> postinglists,int phrasePos) {
        int[] positions = new int[postinglists.size()];
        boolean success = true;
        for (int i = 0; i < positions.length; i++)
        {
            positions[i] = nextPos(postinglists.get(i), docId, pos,phrasePos,i);
            if (positions[i] < 0)
                return -1;
        }
        // int maximum=positions[0];
        for (int i = 1; i < positions.length; i++) {
            if (positions[i] != positions[i - 1] + 1)
                success = false;
            // if (positions[i]>maximum)
            // maximum=positions[i];
        }
        if (success == true)
            return positions[0];
        else
            return nextPhrase(docId, positions[0], postinglists,phrasePos);
    }

    private static int nextPhrase(int docId, Vector<List<Integer>> postinglists,int i) {
        int docVerify = nextForOccurence(docId, postinglists,i);
        // System.out.println("docVerify is: "+docVerify);
        if (docVerify < 0)
            return -1;
        int result = nextPhrase(docVerify, -1, postinglists,i);
        if (result > 0)
            return docVerify;
        return nextPhrase(docVerify, postinglists,i);
    }

    private static int next(int docId, Vector<Vector<List<Integer>>> postinglists) {
        // System.out.println("current id is: "+docId);
        int previousVal = -1;
        boolean equilibrium = true;
        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < postinglists.size(); i++) {
            int currentId = nextPhrase(docId, postinglists.get(i),i);
            if (currentId < 0)
                return -1;
            if (previousVal < 0) {
                previousVal = currentId;
                maximum = currentId;
            }
            else {
                if (previousVal != currentId) {
                    equilibrium = false;
                    maximum = Math.max(maximum, currentId);
                }
            }
        }
        if (equilibrium == true)
            return previousVal;
        else
            return next(maximum - 1, postinglists);
    }
    private boolean canUseCache(Query query, int docid){
    	if (query._query.equals(previousQuery)==false)
    		return false;
    	if (docid<=previousDocid)
    		return false;
    	return true;
    }
    @Override
    public Document nextDoc(Query query, int docid) {
        Vector<String> tokens = query._tokens;
        int result = -1;
        Vector<Vector<List<Integer>>> postingLists = new Vector<Vector<List<Integer>>>();
        for (int i = 0; i < tokens.size(); i++) {
            Vector<List<Integer>> container = new Vector<List<Integer>>();
            String[] consecutiveWords = tokens.get(i).split(" ");
            for (int j = 0; j < consecutiveWords.length; j++) {
                Stemmer s = new Stemmer();
                s.add(consecutiveWords[j].toLowerCase().toCharArray(), consecutiveWords[j].length());
                s.stem();
                container.add(ivtGet(s.toString()));
            }
            // System.out.println("size is: "+docInvertedMap.get(s.toString()).size());
            postingLists.add(container);
        }
        if (canUseCache(query,docid)==false)
        {
        	previousQuery=query._query;
        	previousDocid=docid;
        	cachePos=new Vector<Vector<Integer>> ();
        	for (int i=0;i<tokens.size();i++){
        		Vector<Integer> tempVec=new Vector<Integer> ();
        		int size=postingLists.get(i).size();
        		for (int j=0;j<size;j++)
        			tempVec.add(0);
        		cachePos.add(tempVec);
        	}
        }
        result = next(docid, postingLists);
        // System.out.println("the result is:"+result);
        if (result < 0)
            return null;
        else
            return getDoc(result);
    }

    @Override
    public int corpusDocFrequencyByTerm(String term) {
        // Number of documents in which {@code term} appeared, over the full
        // corpus.

        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());
        s.stem();

        if (!ivtContainsKey(s.toString())) {
            return 0;
        }

        // Get posting list from index.
        List<Integer> l = ivtGet(s.toString());
        int count = 0;
        int last_id = -1;
        for (int i = 0; i < l.size() - 1; i += 2) {
            if (l.get(i) != last_id) {
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
        s.stem();

        if (!ivtContainsKey(s.toString())) {
            return 0;
        }

        // Get posting list from index.
        List<Integer> l = ivtGet(s.toString());

        return l.size() / 2;
    }

    @Override
    public int documentTermFrequency(String term, int docid) {
        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());
        s.stem();

        Map<String, Integer> tfMap = tfm.gettermFrequencyForDoc(docid);

        if (!tfMap.containsKey(s.toString())) {
            return 0;
        }

        return tfMap.get(s.toString());
    }

    private boolean ivtContainsKey(String key) {
        for (Map<String, List<Integer>> m : ivtIndexMapList) {
            if (m.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, List<Integer>> cache = new HashMap<String, List<Integer>>();

    private List<Integer> ivtGet(String key) {
        if (cache.containsKey(key)) {
            return cache.get(key);
        }
        if (cache.size() > 1) {
            cache.remove(cache.keySet().toArray()[0]);
        }
        List<Integer> l = new ArrayList<Integer>();
        for (Map<String, List<Integer>> m : ivtIndexMapList) {
            if (m.containsKey(key)) {
                l.addAll(m.get(key));
            }
        }
        cache.put(key, l);
        return l;
    }

    @Override
    public Map<String, Integer> documentTermFrequencyMap(int docid) {
        return tfm.gettermFrequencyForDoc(docid);
    }

    public static void main(String[] args) {
    }
}
