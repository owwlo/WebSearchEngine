
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
public class IndexerInvertedDoconly extends Indexer {
    private List<IvtMapInteger> ivtIndexMapList = new ArrayList<IvtMapInteger>();

    private Map<Integer, DocumentIndexed> docMap = null;
    private Map<String, Integer> docUrlMap = null;
    private Map<String, Object> infoMap = null;

    // Table name for index of documents.
    private static final String DOC_IDX_TBL = "docDB";
    private static final String DOC_URL_TBL = "docUrlDB";
    private static final String DOC_INFO_TBL = "docInfoDB";

    private TermFrequencyManager tfm;

    public IndexerInvertedDoconly(Options options) {
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
        private Map<String, List<Integer>> ivtMap = new HashMap<String, List<Integer>>();
        private long termCount = 0;

        public InvertIndexBuildingTask(List<File> files, int startFileIdx, int endFileIdx) {
            this.files = files;
            this.startFileIdx = startFileIdx;
            this.endFileIdx = endFileIdx;
        }

        public Map<String, List<Integer>> getIvtMapForThread() {
            return ivtMap;
        }

        public long getTermCount() {
            return termCount;
        }

        @Override
        public void run() {
            System.out.println("Thread " + Thread.currentThread().getName()
                    + " processes files from " + startFileIdx
                    + " to " + endFileIdx);
            for (int docId = startFileIdx; docId < endFileIdx; docId++) {
                File file = files.get(docId);
                Map<String, Integer> ivtMapItem = new HashMap<String, Integer>();
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
                        ivtMapItem.put(token, 0);
                    }
                    ivtMapItem.put(token, ivtMapItem.get(token) + 1);
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
                        ivtMap.put(token, new LinkedList<Integer>());
                    }
                    List<Integer> recordList = ivtMap.get(token);
                    recordList.add(docId);
                    recordList.add(ivtMapItem.get(token));
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

        int threadCount = 1;

        System.out.println("Start building index with " + threadCount + " threads. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

        long termCount = 0;

        infoMap = new HashMap<String, Object>();
        docMap = new HashMap<Integer, DocumentIndexed>();
        docUrlMap = new HashMap<String, Integer>();

        infoMap.put("_numDocs", files.size());

        InvertedIndexBuilder builder = InvertedIndexBuilder.getBuilder(new File(
                _options._indexPrefix));

        tfm = new TermFrequencyManager(_options._indexPrefix);

        int filesPerBatch = 2000;

        for (int batchNum = 0; batchNum < files.size() / filesPerBatch + 1; batchNum++) {
            int fileIdStart = batchNum * filesPerBatch;
            int fileIdEnd = (batchNum + 1) * filesPerBatch;
            if (fileIdEnd > files.size()) {
                fileIdEnd = files.size();
            }

            System.out.println("Processing files from " + fileIdStart + " to " + fileIdEnd);

            ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);

            IvtMapInteger ivtMapFile = builder.createDistributedIvtiIntegerMap();

            List<InvertIndexBuildingTask> taskList = new ArrayList<InvertIndexBuildingTask>();

            int totalFileCount = fileIdEnd - fileIdStart;
            int filesPerThread = totalFileCount / threadCount;
            for (int threadId = 0; threadId < threadCount; threadId++) {
                int startFileIdx = threadId * filesPerThread + fileIdStart;
                int endFileIdx = (threadId + 1) * filesPerThread + fileIdStart;
                if (threadId == threadCount - 1) {
                    endFileIdx = fileIdEnd;
                }
                InvertIndexBuildingTask iibt = new InvertIndexBuildingTask(files, startFileIdx,
                        endFileIdx);
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
                ivtMapFile.putAll(iibt.getIvtMapForThread());
                termCount += iibt.getTermCount();
            }

            ivtMapFile.close();

            System.out.println("Batch commit done. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

            System.out.println("Batch commit done.");
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

        System.out.println("Construct done. Duration: " + (end_t - start_t) / 1000.0 + "s");
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
     * In HW2, you should be using {@link DocumentIndexed}
     */

    private static int nextForOne(int docId, List<Integer> postinglists) {
        if (postinglists == null)
            return -1;
        if (postinglists.size() < 1 || postinglists.get(postinglists.size() - 2) <= docId)
            return -1;
        if (postinglists.get(0) > docId)
            return postinglists.get(0);
        int start = 0;
        int end = postinglists.size() / 2 - 1;
        while (end - start > 1) {
            int mid = (start + end) / 2;
            if (postinglists.get(mid * 2) <= docId)
                start = mid;
            else
                end = mid;
        }
        return postinglists.get(2 * end);
    }

    public static int next(int docId, Vector<List<Integer>> postinglists) {
        // System.out.println("current id is: "+docId);
        int previousVal = -1;
        boolean equilibrium = true;
        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < postinglists.size(); i++) {
            int currentId = nextForOne(docId, postinglists.get(i));
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

    @Override
    public Document nextDoc(Query query, int docid) {
        // query.processQuery();
        Vector<String> tokens = query._tokens;
        int result = -1;
        Vector<List<Integer>> postingLists = new Vector<List<Integer>>();
        for (int i = 0; i < tokens.size(); i++) {
            Stemmer s = new Stemmer();
            s.add(tokens.get(i).toLowerCase().toCharArray(), tokens.get(i).length());
            s.stem();
            // System.out.println("size is: "+docInvertedMap.get(s.toString()).size());
            postingLists.add(ivtGet(s.toString()));
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
    public int corpusTermFrequency(String term) {
        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());
        s.stem();
        // System.out.println("term is: "+ term+ "stemmer is: "+s.toString());
        if (!ivtContainsKey(s.toString())) {
            // System.out.println("it is not contained...");
            return 0;
        }
        // System.out.println("OMG....");
        // Get posting list from index.
        List<Integer> l = ivtGet(s.toString());

        int result = 0;
        for (int i = 0; i < l.size() / 2; i++) {
            result += l.get(i * 2 + 1);
        }

        return result;
    }

    private static int bsInner(final int start, final int end, final int docid,
            final List<Integer> list) {
        int Start = start;
        int End = end;
        while (Start <= End) {
            int mid = (Start + End) / 2;
            if (docid == list.get(2 * mid))
                return (2 * mid);
            if (docid < list.get(2 * mid))
                End = mid - 1;
            else
                Start = mid + 1;
            // System.out.println("Start is: "+Start+"End is :"+End);
        }
        return -1;
    }

    private int binarySearchPostList(final int docId, final List<Integer> list) {
        // return bsInner(0, list.size() - 1, docId, list);
        // Modification:
        return bsInner(0, list.size() / 2 - 1, docId, list);
    }

    @Override
    public int documentTermFrequency(String term, int docid) {
        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());
        s.stem();

        if (!ivtContainsKey(s.toString())) {
            return 0;
        }
        // Get posting list from index.
        List<Integer> l = ivtGet(s.toString());

        // Use binary search looking for docid within given posting list.
        int pos = binarySearchPostList(docid, l);
        if (pos != -1) {
            // Return term frequency for given doc and term
            // System.out.println("current num is: "+l.get(pos+1));
            return l.get(pos + 1);
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

    private Map<String, List<Integer>> cache = new LinkedHashMap<String, List<Integer>>();

    private List<Integer> ivtGet(String key) {
        if (cache.containsKey(key)) {
            List<Integer> tmp = cache.get(key);
            cache.remove(tmp);
            cache.put(key, tmp);
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
