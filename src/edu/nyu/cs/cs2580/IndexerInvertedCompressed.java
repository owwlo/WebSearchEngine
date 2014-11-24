
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
import org.owwlo.InvertedIndexing.InvertedIndexBuilder.IvtMapByte;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.utils.PersistentStoreManager;
import edu.nyu.cs.cs2580.utils.PersistentStoreManager.TermFrequencyManager;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends Indexer {
    private List<IvtMapByte> ivtIndexMapList = new ArrayList<IvtMapByte>();

    private Map<Integer, DocumentIndexed> docMap = null;
    private Map<String, Integer> docUrlMap = null;
    private Map<String, Object> infoMap = null;

    // Table name for index of documents.
    private static final String DOC_IDX_TBL = "docDB";
    private static final String DOC_INFO_TBL = "docInfoDB";
    private static final String DOC_URL_TBL = "docUrlDB";

    private TermFrequencyManager tfm;

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
        private Map<String, List<Byte>> ivtMap;
        private long termCount = 0;

        public InvertIndexBuildingTask(List<File> files, int startFileIdx,
                int endFileIdx, Map<String, List<Byte>> ivtMap) {
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

                    token = s.toString();

                    if (token.length() < 1 || token.length() > 20) {
                        continue;
                    }

                    if (!ferqMap.containsKey(token)) {
                        ferqMap.put(token, 0);
                    }
                    ferqMap.put(token, ferqMap.get(token) + 1);

                    if (!ivtMapItem.containsKey(token)) {
                        ArrayList<Integer> occList = new ArrayList<Integer>();
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
                        ivtMap.put(token, new ArrayList<Byte>());
                    }
                    List<Byte> recordList = ivtMap.get(token);

                    List<Integer> occList = ivtMapItem.get(token);
                    List<Byte> _docId = IndexerInvertedCompressed
                            .compressInt(docId); // get the compressed id

                    // sequentially add <docid, occurrence> to the posting list.
                    for (int e : occList) {
                        recordList.addAll(_docId);

                        ArrayList<Byte> _occ = compressInt(e);
                        recordList.addAll(_occ);
                    }
                }

                buildDocumentIndex(di);
            }
        }
    }

    public IndexerInvertedCompressed(Options options) {
        super(options);
        System.out.println("Using Indexer: " + this.getClass().getSimpleName());
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

            IvtMapByte ivtMapFile = builder.createDistributedIvtiByteMap();
            Map<String, List<Byte>> ivtMap = new HashMap<String, List<Byte>>();

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
            int docid = die.getKey();
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
        InvertedIndexBuilder builder = InvertedIndexBuilder.getBuilder(new File(
                _options._indexPrefix));

        tfm = new TermFrequencyManager(_options._indexPrefix);

        IvtMapByte ivtMapBatch = builder.getUnifiedDistributedIvtiByteMap();
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

    private String previousQuery = new String();
    private int previousDocid = -1;
    private Vector<Vector<Integer>> cachePos = new Vector<Vector<Integer>>();

    private int nextInOccurence(int docId, List<Integer> postinglist, int phraseIndex, int termIndex) {
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

    private int nextForOccurence(int docId, Vector<List<Integer>> postinglists, int phraseIndex) {
        // System.out.println("current id is: "+docId);
        int previousVal = -1;
        boolean equilibrium = true;
        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < postinglists.size(); i++) {
            int currentId = nextInOccurence(docId, postinglists.get(i), phraseIndex, i);
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
            return nextForOccurence(maximum - 1, postinglists, phraseIndex);
    }

    private int nextPos(List<Integer> postinglist, int docId, int pos, int phrasePos, int termPos) {
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

    private int nextPhrase(int docId, int pos, Vector<List<Integer>> postinglists, int phrasePos) {
        int[] positions = new int[postinglists.size()];
        boolean success = true;
        for (int i = 0; i < positions.length; i++)
        {
            positions[i] = nextPos(postinglists.get(i), docId, pos, phrasePos, i);
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

    private boolean canUseCache(Query query, int docid) {
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
                container.add(decompressArray(ivtGet(s.toString())));
            }
            postingLists.add(container);
        }
        if (canUseCache(query, docid) == false)
        {

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
        List<Byte> l = ivtGet(s.toString());

        int count = 0;
        ArrayList<Byte> last_code = new ArrayList<Byte>();
        for (int i = 0; i < l.size();) {

            // get all the bytes of docid
            ArrayList<Byte> code = new ArrayList<Byte>();
            byte currByte = l.get(i);
            while ((currByte & 0x80) == (byte) 0) {
                code.add(currByte);
                currByte = l.get(i++);
            }
            code.add(currByte);
            i++;

            if (!last_code.equals(code)) {
                last_code = code;
                ++count;
            }

            while ((l.get(i) & 0x80) == (byte) 0) {
                i++;
            }
            i++;
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
        List<Byte> l = ivtGet(s.toString());

        int result = 0;
        for (int i = 0; i < l.size();) {
            while ((l.get(i) & 0x80) == (byte) 0) {
                i++;
            }
            i++;

            result++;
        }

        return result / 2;
    }

    // do linear search of a docid for compressed posting list, first occurrence
    private int linearSearchPostDecompressed(final int docId, final List<Integer> list) {
        int i = 0;
        int pos = -1;
        while (i < list.size()) {
            pos = i;

            if (list.get(i) == docId) {
                return pos;
            }
            i++; // skip the occurrence
            i++; // go to the next docid
        }
        return -1;
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
        List<Byte> l = ivtGet(s.toString());
        ArrayList<Integer> arr = decompressArray(l);

        // Use binary search looking for docid within given posting list.
        int pos = linearSearchPostDecompressed(docid, arr);

        if (pos != -1) {
            // Return term frequency for given doc and term
            int count = 0;
            while (pos < arr.size() - 1 && arr.get(pos) == docid) {
                ++count;
                pos += 2;
            }
            return count;
        } else {
            return 0;
        }
    }

    private boolean ivtContainsKey(String key) {
        for (Map<String, List<Byte>> m : ivtIndexMapList) {
            if (m.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, List<Byte>> cache = new HashMap<String, List<Byte>>();

    private List<Byte> ivtGet(String key) {
        if (cache.containsKey(key)) {
            return cache.get(key);
        }
        if (cache.size() > 1) {
            cache.remove(cache.keySet().toArray()[0]);
        }
        List<Byte> l = new ArrayList<Byte>();
        for (Map<String, List<Byte>> m : ivtIndexMapList) {
            if (m.containsKey(key)) {
                l.addAll(m.get(key));
            }
        }
        cache.put(key, l);
        return l;
    }

    // provide all bytes of a docid to get a int
    static public int decompressBytes(ArrayList<Byte> code) {
        int res = 0;
        for (int i = code.size() - 1; i >= 0; --i) {
            res += (code.get(i) & 0x7f) << (7 * (code.size() - 1 - i));
        }
        return res;
    }

    // provide array of bytes, return array of ints
    static public ArrayList<Integer> decompressArray(List<Byte> list) {
        ArrayList<Integer> decode = new ArrayList<Integer>();

        int i = 0;
        while (i < list.size()) {
            // get the docid or occurrence
            ArrayList<Byte> item = new ArrayList<Byte>();
            while ((list.get(i) & 0x80) == (byte) 0) {
                item.add(list.get(i));
                i++;
                if (i >= list.size()) {
                    System.out.println("Error: illegal code!");
                    return null;
                }
            }
            item.add(list.get(i));
            i++;

            // decompress the docid
            decode.add(decompressBytes(item));
        }

        return decode;
    }

    // provide docid/occurrence to bytes of codes
    static public ArrayList<Byte> compressInt(int num) {
        int digits = 0;
        if (num >= 0 && num < 128) {
            digits = 1;
        } else if (num < 16384) {
            digits = 2;
        } else if (num < 2097152) {
            digits = 3;
        } else if (num < 268435456) {
            digits = 4;
        } else {
            digits = 5;
            // System.out.println("!! five digits !!");
        }

        ArrayList<Byte> res = new ArrayList<Byte>(digits);
        for (int i = 0; i < digits - 1; ++i) {
            res.add((byte) (0x7f & (num >> (7 * (digits - 1 - i)))));
        }
        res.add((byte) (0x7f & num | 0x80));
        return res;
    }

    public static void main(String args[]) {
    }

    @Override
    public Map<String, Integer> documentTermFrequencyMap(int docid) {
        return tfm.gettermFrequencyForDoc(docid);
    }
}
