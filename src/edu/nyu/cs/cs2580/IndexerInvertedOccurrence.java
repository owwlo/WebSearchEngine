
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.jsoup.Jsoup;
import org.owwlo.InvertedIndexing.InvertedIndexBuilder;
import org.owwlo.InvertedIndexing.InvertedIndexBuilder.IvtMapInteger;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.utils.AssistantIndexBuilder;
import edu.nyu.cs.cs2580.utils.PersistentStoreManager;
import edu.nyu.cs.cs2580.utils.Utils;
import edu.nyu.cs.cs2580.utils.PersistentStoreManager.TermFrequencyManager;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedOccurrence extends Indexer {
    private List<IvtMapInteger> ivtIndexMapList = new ArrayList<IvtMapInteger>();

    private Map<Integer, DocumentIndexed> docMap = null;
    private Map<String, Integer> docUrlMap = null;
    private Map<String, Object> infoMap = null;
    private String CorpusLocation = "data/wiki";
    private AssistantIndexBuilder aib = null;

    private String previousQuery = new String();
    private int previousDocid = -1;
    private Vector<Vector<Integer>> cachePos = new Vector<Vector<Integer>>();

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
        private AssistantIndexBuilder aib;

        public InvertIndexBuildingTask(List<File> files, int startFileIdx,
                int endFileIdx, Map<String, List<Integer>> ivtMap,
                AssistantIndexBuilder aib) {
            this.files = files;
            this.startFileIdx = startFileIdx;
            this.endFileIdx = endFileIdx;
            this.ivtMap = ivtMap;
            this.aib = aib;
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

                List<String> termList = new ArrayList<String>();

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
                    termList.add(token);
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

                // aib.buildDocTermPositionMap(docId, termList);

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
        // AssistantIndexBuilder.cleanFiles(_options);

        // Get all corpus files.
        List<File> files = getAllFiles(new File(corpusFolder));

        int filesPerBatch = 1300;

        // initialStore(false, files.size() / filesPerBatch);

        int threadCount = 1;

        System.out.println("Start building index with " + threadCount
                + " threads. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

        infoMap = new HashMap<String, Object>();
        docMap = new HashMap<Integer, DocumentIndexed>();
        docUrlMap = new HashMap<String, Integer>();

        infoMap.put("_numDocs", files.size());

        InvertedIndexBuilder builder = InvertedIndexBuilder
                .getBuilder(new File(_options._indexPrefix));
        AssistantIndexBuilder aib = AssistantIndexBuilder.getInstance(_options);

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

            IvtMapInteger ivtMapFile = builder
                    .createDistributedIvtiIntegerMap();
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
                        files, startFileIdx, endFileIdx, ivtMap, aib);
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
            // di.setPageRank((float) (double) pageRankMap.get(basename));
            // di.setNumViews((int) (double) numViewsMap.get(basename));
        }

        // aib.close();

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
        docUrlMap = (Map<String, Integer>) PersistentStoreManager
                .readObjectFromFile(docUrlFile);
        infoMap = (Map<String, Object>) PersistentStoreManager
                .readObjectFromFile(docInfoFile);

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
        InvertedIndexBuilder builder = InvertedIndexBuilder
                .getBuilder(new File(_options._indexPrefix));

        tfm = new TermFrequencyManager(_options._indexPrefix);

        IvtMapInteger ivtMapBatch = builder
                .getUnifiedDistributedIvtiIntegerMap();
        ivtIndexMapList.add(ivtMapBatch);
        aib = AssistantIndexBuilder.getInstance(_options);

        // AssistantIndexBuilder aib =
        // AssistantIndexBuilder.getInstance(_options);
        // aib.buildTwoLetterMap(ivtMapBatch.keySet());
        // aib.buildTermLenMap(ivtMapBatch.keySet());
        //
        // String a = aib.getTermPositionManager().getTermAtPosition(1, 1);
        // System.out.println("a: " + a);
        // List<String> b =
        // aib.getTwoLetterTermListManager().getLetterTermList("or");
        // for (String s : b) {
        // System.out.println(s);
        // }
        // List<String> c = aib.getTermLenListManager().getTermLenList(2);
        // for (String s : c) {
        // System.out.println(s);
        // }

        readVariables();
    }

    @Override
    public Document getDoc(int docid) {
        return docMap.get(docid);
    }

    /**
     * In HW2, you should be using {@link DocumentIndexed}.
     */
    private int nextInOccurence(int docId, List<Integer> postinglist,
            int phraseIndex, int termIndex) {
        int start = cachePos.get(phraseIndex).get(termIndex);
        for (int i = start; i < postinglist.size(); i += 2) {
            if (postinglist.get(i) > docId) {
                cachePos.get(phraseIndex).set(termIndex, i);
                return postinglist.get(i);
            }
        }
        cachePos.get(phraseIndex).set(termIndex, postinglist.size());
        return -1;
    }

    private int nextForOccurence(int docId, Vector<List<Integer>> postinglists,
            int phraseIndex) {
        // System.out.println("current id is: "+docId);
        int previousVal = -1;
        boolean equilibrium = true;
        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < postinglists.size(); i++) {
            int currentId = nextInOccurence(docId, postinglists.get(i),
                    phraseIndex, i);
            if (currentId < 0)
                return -1;
            if (previousVal < 0) {
                previousVal = currentId;
                maximum = currentId;
            } else {
                if (previousVal != currentId) {
                    equilibrium = false;
                    maximum = Math.max(maximum, currentId);
                }
            }
        }
        if (equilibrium == true)
            return previousVal;
        else
            return nextForOccurence(maximum - 1, postinglists, phraseIndex);
    }

    private int nextPos(List<Integer> postinglist, int docId, int pos,
            int phrasePos, int termPos) {
        int docPosition = -1;
        int start = cachePos.get(phrasePos).get(termPos);
        for (int i = start; i < postinglist.size(); i += 2) {
            if (postinglist.get(i) == docId) {
                docPosition = i;
                cachePos.get(phrasePos).set(termPos, i);
                break;
            }
        }
        if (docPosition == -1) {
            cachePos.get(phrasePos).set(termPos, postinglist.size());
            return -1;
        }
        int Pos = docPosition + 1;
        while (Pos < postinglist.size() && postinglist.get(Pos - 1) == docId) {
            if (postinglist.get(Pos) > pos) {
                cachePos.get(phrasePos).set(termPos, Pos - 1);
                return postinglist.get(Pos);
            }
            Pos += 2;
        }
        cachePos.get(phrasePos).set(termPos, postinglist.size());
        return -1;
    }

    private int nextPhrase(int docId, int pos,
            Vector<List<Integer>> postinglists, int phrasePos) {
        int[] positions = new int[postinglists.size()];
        boolean success = true;
        for (int i = 0; i < positions.length; i++) {
            positions[i] = nextPos(postinglists.get(i), docId, pos, phrasePos,
                    i);
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
            return nextPhrase(docId, positions[0], postinglists, phrasePos);
    }

    private int nextPhrase(int docId, Vector<List<Integer>> postinglists, int i) {
        int docVerify = nextForOccurence(docId, postinglists, i);
        // System.out.println("docVerify is: "+docVerify);
        if (docVerify < 0)
            return -1;
        int result = nextPhrase(docVerify, -1, postinglists, i);
        if (result > 0)
            return docVerify;
        return nextPhrase(docVerify, postinglists, i);
    }

    private int next(int docId, Vector<Vector<List<Integer>>> postinglists) {
        // System.out.println("current id is: "+docId);
        int previousVal = -1;
        boolean equilibrium = true;
        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < postinglists.size(); i++) {
            int currentId = nextPhrase(docId, postinglists.get(i), i);
            if (currentId < 0)
                return -1;
            if (previousVal < 0) {
                previousVal = currentId;
                maximum = currentId;
            } else {
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

    private boolean canUseCache(Query query, int docid) {
        if (docid == -1) {
            return false;
        }
        if (query._query.equals(previousQuery) == false) {
            return false;
        }

        if (docid <= previousDocid) {
            return false;
        }

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
                s.add(consecutiveWords[j].toLowerCase().toCharArray(),
                        consecutiveWords[j].length());
                s.stem();
                container.add(ivtGet(s.toString()));
            }
            // System.out.println("size is: "+docInvertedMap.get(s.toString()).size());
            postingLists.add(container);
        }
        if (canUseCache(query, docid) == false) {

            previousQuery = query._query;
            previousDocid = -1;
            cachePos = new Vector<Vector<Integer>>();
            for (int i = 0; i < postingLists.size(); i++) {
                Vector<Integer> tempVec = new Vector<Integer>();
                int size = postingLists.get(i).size();
                for (int j = 0; j < size; j++)
                    tempVec.add(0);
                cachePos.add(tempVec);
            }
        }
        result = next(docid, postingLists);
        previousDocid = result - 1;
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

    public int listLength(String key) {
        return ivtGet(key).size();
    }

    private void addPQ(PriorityQueue<Word> pq, Word word, int windowSize) {
        if (pq.size() < windowSize) {

            if (word.frequency > 2000)
                pq.add(word);
        } else {
            if (word.compareTo(pq.peek()) > 0 && word.frequency > 2000) {
                pq.poll();
                // System.out.println(word.term+" "+word.frequency);
                pq.add(word);
            }
        }
    }

    private boolean allChars(String k) {
        for (int i = 0; i < k.length(); i++) {
            if (Character.isAlphabetic(k.charAt(i)) == false)
                return false;
        }
        return true;
    }

    private void storeCandidate(Query query, Vector<Vector<Word>> temp,
            int windowSize) {

        for (int i = 0; i < query._tokens.size(); i++) {
            long currentTime = System.currentTimeMillis();
            String target = query._tokens.get(i);
            PriorityQueue<Word> pq = new PriorityQueue<Word>();
            Set<String> candidates = getPossible(target);
            Iterator<String> it = candidates.iterator();
            while (it.hasNext()) {
                String k = it.next();
                if (Math.abs(target.length() - k.length()) >= 3)
                    continue;
                if (k.equals(target) == true) {
                    int listLength = ivtGet(k).size();
                    temp.get(i).add(new Word(target, listLength));
                    continue;
                }
                if (allChars(k) == false)
                    continue;
                if (Utils.wordDistance(target, k) <= 1) {
                    int listLength = ivtGet(k).size();
                    Word word = new Word(k, listLength);
                    addPQ(pq, word, windowSize);

                }
            }
            while (pq.isEmpty() == false) {
                temp.get(i).add(pq.poll());
            }
            long timeElapsed = System.currentTimeMillis() - currentTime;
            System.out.println("time elapsed: " + timeElapsed);
        }

    }

    private void makeQueryList(Vector<Vector<Word>> Combinations,
            List<Query> candidates, String[] temp, int position) {
        if (position >= Combinations.size()) {

            String result = temp[0];
            for (int i = 1; i < position; i++) {

                result += " ";
                result += temp[i];

            }
            Query query = new Query(result);
            query.processQuery();
            candidates.add(query);
            return;
        }

        Vector<Word> current = Combinations.get(position);
        for (int i = 0; i < current.size(); i++) {
            temp[position] = current.get(i).term;
            makeQueryList(Combinations, candidates, temp, position + 1);

        }

    }

    private void experiment(List<Query> lists, List<Query> result) {

        for (Query q : lists) {
            int count = 0;
            int docid = -1;
            DocumentIndexed doc;
            while ((doc = (DocumentIndexed) nextDoc(q, docid)) != null) {
                if (count >= 3) {
                    result.add(q);
                    break;
                }
                count++;
                docid = doc._docid;
                // System.out.println("current docid: "+docid);
            }

        }
    }

    public void refineCandidates(Vector<Vector<Word>> candidates, Query query) {
        for (int i = 0; i < candidates.size(); i++) {
            Vector<Word> vs = candidates.get(i);
            String target = query._tokens.get(i);
            Iterator<Word> it = vs.iterator();
            int size = vs.size();
            while (it.hasNext() == true) {
                Word current = it.next();
                if (current.term.equals(target) == true) {
                    if ((size >= 2) && (current.frequency < 2000)) {
                        it.remove();
                    }
                }
            }
        }
    }

    public Set<String> getPossible(String str) {
        Set<String> result = new HashSet<String>();
        long currentTime = System.currentTimeMillis();
        int len = str.length();
        if (len == 1) {
            result.add(str);
            return result;
        }
        Map<String, Integer> tempMap = new HashMap<String, Integer>();
        for (int i = 0; i + 2 <= len; i++) {
            String biGram = str.substring(i, i + 2);
            List<String> biGramList = aib.getTwoLetterTermListManager()
                    .getLetterTermList(biGram);
            if (biGramList == null)
                biGramList = new ArrayList<String>();
            for (String element : biGramList) {
                if (tempMap.containsKey(element) == false)
                    tempMap.put(element, 1);
                else
                    tempMap.put(element, tempMap.get(element) + 1);
            }
        }
        double boundary = 0.0;
        if (len <= 5)
            boundary = 1;
        else
            boundary = 2;
        // int boundaryAsInt = (int)boundary;
        for (String element : tempMap.keySet()) {
            // System.out.println(element);
            int tmp = tempMap.get(element);
            double tmpDou = (double) tmp;
            if (tmpDou >= boundary) {
                result.add(element);

                // System.out.println("advertisement");
            }
        }
        long offSet = System.currentTimeMillis() - currentTime;
        // System.out.println(offSet);
        return result;
    }

    public Map<String,List<Query>> querySearch(Query query) {

        /*
         * Map<String,Integer> result= supplementWord(query); for (String e:
         * result.keySet()){ System.out.println(e); } return null;
         */
    	Map<String,List<Query>> finalResult = new HashMap<String,List<Query>>();
    	List<Query> historySub = new ArrayList<Query> ();
        SessionHandler sh = SessionHandler.getInstance();
        List<String> history =sh.queryAllHistory();
        for (String str:history){
        	if (str.toLowerCase().contains(query._query.toLowerCase())==true){
        		Query qq = new Query(str);
        		qq.processQuery();
        		historySub.add(qq);
        	}
        }
        finalResult.put("history", historySub);
        
        long currentTime = System.currentTimeMillis();
        int windowSize = 2;
        Vector<Vector<Word>> temp = new Vector<Vector<Word>>();
        for (int i = 0; i < query._tokens.size(); i++)
            temp.add(new Vector<Word>());
        storeCandidate(query, temp, windowSize);
        refineCandidates(temp, query);
        List<Query> candidates = new ArrayList<Query>();
        String[] tempQuery = new String[temp.size()];
        makeQueryList(temp, candidates, tempQuery, 0);
        // for (int i=0;i<candidates.size();i++)
        // System.out.println(candidates.get(i));
        // long elapse = System.currentTimeMillis() - currentTime;
        // System.out.println("elapsed: "+elapse);
        System.out.println(candidates.size());
        List<Query> result = new ArrayList<Query>();
        if (query._tokens.size() < 4)
            experiment(candidates, result);
        else
            for (Query candidate : candidates) {
                result.add(candidate);
            }
        // elapse = System.currentTimeMillis() - currentTime;
        // System.out.println("elapsed: "+elapse);
        List<wordQ> tmp = new ArrayList<wordQ>();
        for (int i = 0; i < result.size(); i++) {
            // System.out.println(result.get(i));
            int distance = 0;
            for (int j = 0; j < query._tokens.size(); j++) {
                if (query._tokens.get(j).charAt(0) != result.get(i)._tokens.get(j).charAt(0)) {
                    distance += (query._tokens.size() - j);
                }
            }
            // System.out.println(result.get(i)+"distance is: "+distance);
            tmp.add(new wordQ(result.get(i), distance));
        }

        Collections.sort(tmp);
        long elapse = System.currentTimeMillis() - currentTime;
        System.out.println("elapsed: " + elapse);

        for (int i = 0; i < tmp.size(); i++) {
            result.set(i, tmp.get(i).term);
        }
        for (int i = 0; i < result.size(); i++)
            System.out.println("result: " + result.get(i));
        finalResult.put("correction", result);
        
        
        List<Query> suggestions = new ArrayList<Query> ();
        for (int i=0;i<result.size();i++){
        	List<String> possibles = new ArrayList<String> ();
        	possibles = sh.querySearch(result.get(i));
        	for (String str: possibles){
        		Query strQ = new Query(str);
        		strQ.processQuery();
        		suggestions.add(strQ);
        	}
        }
        finalResult.put("suggestions", suggestions);
        return finalResult;

    }

    public String nextFollowing(Query q, int docid, int position) {

        for (int i = 0; i < q._tokens.size(); i++) {

            String toCompare = q._tokens.get(i);
            // System.out.println(toCompare);
            String cur = aib.getTermPositionManager().getTermAtPosition(docid, position + i);
            // System.out.println(cur);
            if (cur == null)
                return null;
            if (cur.equals(toCompare) == false)
                return "";
        }
        return aib.getTermPositionManager().getTermAtPosition(docid, position + q._tokens.size());
    }

    public Map<String, Integer> supplementWord(Query q) {
        System.out.println("i am here.....");
        DocumentIndexed doc;
        int docid = -1;
        Map<String, Integer> mapping = new HashMap<String, Integer>();
        int count = 0;
        while ((doc = (DocumentIndexed) nextDoc(q, docid)) != null) {

            if (count >= 20) {
                break;
            }
            docid = doc._docid;
            // System.out.println(docid);
            for (int position = 1;; position++) {
                String k = nextFollowing(q, docid, position);
                if (k == null)
                    break;
                if (k.length() > 0) {
                    if (mapping.containsKey(k) == false)
                        mapping.put(k, 1);
                    else
                        mapping.put(k, mapping.get(k) + 1);
                }
            }
            count++;
            // System.out.println("current docid: "+docid);
        }
        return mapping;
    }

    public static void main(String[] args) {

    }
}

class wordQ implements Comparable<wordQ> {

    public Query term;
    public int frequency;

    @Override
    public int compareTo(wordQ w) {
        if (frequency < w.frequency) {
            return -1;
        }
        if (frequency > w.frequency) {
            return 1;
        }
        return 0;
    }

    public wordQ(Query term, int f) {
        this.term = term;
        this.frequency = f;
    }

}

class Word implements Comparable<Word> {
    public String term;
    public int frequency;

    @Override
    public int compareTo(Word w) {
        if (frequency < w.frequency) {
            return -1;
        }
        return 1;
    }

    public Word(String term, int f) {
        this.term = term;
        this.frequency = f;
    }
}
