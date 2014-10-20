
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

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;

import edu.nyu.cs.cs2580.SearchEngine.Options;
import edu.nyu.cs.cs2580.utils.PersistentStoreManager;
import edu.nyu.cs.cs2580.utils.PersistentStoreManager.IvtMapByte;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedCompressed extends Indexer {
    private List<IvtMapByte> ivtIndexMapList = new ArrayList<IvtMapByte>();

    // for compressed posting list
    private Map<String, List<Byte>> compressedPL = new HashMap<String, List<Byte>>();
    // for record in query
    private String currTerm; // current query term
    private Map<Integer, Integer> currPL; // current posting list: <docid,
    // counts>

    private Map<Integer, DocumentIndexed> docMap = null;
    private Map<String, Integer> docUrlMap = null;
    private Map<String, Object> infoMap = null;

    // Table name for index of documents.
    private static final String DOC_IDX_TBL = "docDB";
    private static final String DOC_INFO_TBL = "docInfoDB";
    private static final String DOC_URL_TBL = "docUrlDB";

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
                    // for each token in each doc, add <token, occurrence> to
                    // the
                    // map of <token, {occurrence list}> for each document
                    token = s.toString();

                    if (token.length() < 1 || token.length() > 20) {
                        continue;
                    }

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

                // for each token in each document, add to the map of <token,
                // {docid, occ}>
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

        int filesPerBatch = 1500;

        int threadCount = 1;

        System.out.println("Start building index with " + threadCount
                + " threads. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

        infoMap = new HashMap<String, Object>();
        docMap = new HashMap<Integer, DocumentIndexed>();
        docUrlMap = new HashMap<String, Integer>();

        infoMap.put("_numDocs", files.size());

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

            IvtMapByte ivtMapFile = new IvtMapByte(new File(
                    _options._indexPrefix), "ivt" + batchNum, true);
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

            System.out.println(fileIdEnd
                    + " pages have been processed. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

            // Write ivtMap into storage.
            long recordsCommit = 0;

            System.out.println("Writing Inverted Map to disk. " + fileIdEnd
                    + " pages have been processed. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

            ivtMapFile.putAll(ivtMap);
            ivtMapFile.close();

            System.out.println("Batch commit done. Elapsed: "
                    + (System.currentTimeMillis() - start_t) / 1000.0 + "s");
        }

        infoMap.put("_totalTermFrequency", termCount);

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
        for (int i = 0; i < 100; i++) {
            File file = new File(_options._indexPrefix, "ivt" + i);
            if (!file.exists()) {
                break;
            }
            ivtIndexMapList.add(new IvtMapByte(new File(_options._indexPrefix),
                    "ivt" + i, false));
        }
        readVariables();
    }

    @Override
    public Document getDoc(int docid) {
        return docMap.get(docid);
    }

    /**
     * In HW2, you should be using {@link DocumentIndexed}
     */
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
                container.add(decompressArray(ivtGet(s.toString())));
            }
            // System.out.println("size is: "+docInvertedMap.get(s.toString()).size());
            postingLists.add(container);
        }
        result = next(docid, postingLists);
        // System.out.println("the result is:"+result);
        if (result < 0)
            return null;
        else
            return getDoc(result);
    }

    private static int next(int docId,
            Vector<Vector<List<Integer>>> postinglists) {
        int[] docIds = new int[postinglists.size()];
        // System.out.println("current id is: "+docId);
        int previousVal = -1;
        boolean equilibrium = true;
        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < postinglists.size(); i++) {
            int currentId = nextPhrase(docId, postinglists.get(i));
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

    private static int nextPhrase(int docId, Vector<List<Integer>> postinglists) {
        int docVerify = nextForOccurence(docId, postinglists);
        // System.out.println("docVerify is: "+docVerify);
        if (docVerify < 0)
            return -1;
        int result = nextPhrase(docVerify, -1, postinglists);
        if (result > 0)
            return docVerify;
        return nextPhrase(docVerify, postinglists);
    }

    private static int nextPhrase(int docId, int pos,
            Vector<List<Integer>> postinglists) {
        int[] positions = new int[postinglists.size()];
        boolean success = true;
        for (int i = 0; i < positions.length; i++) {
            positions[i] = nextPos(postinglists.get(i), docId, pos);
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
            return nextPhrase(docId, positions[0], postinglists);
    }

    private static int nextPos(List<Integer> postinglist, int docId, int pos) {
        int docPosition = -1;
        for (int i = 0; i < postinglist.size(); i += 2) {
            if (postinglist.get(i) == docId) {
                docPosition = i;
                break;
            }
        }
        if (docPosition == -1)
            return -1;
        int Pos = docPosition + 1;
        while (Pos < postinglist.size() && postinglist.get(Pos - 1) == docId) {
            if (postinglist.get(Pos) > pos)
                return postinglist.get(Pos);
            Pos += 2;
        }
        return -1;
    }

    private static int nextInOccurence(int docId, List<Integer> postinglist) {
        for (int i = 0; i < postinglist.size(); i += 2) {
            if (postinglist.get(i) > docId)
                return postinglist.get(i);
        }
        return -1;
    }

    private static int nextForOccurence(int docId,
            Vector<List<Integer>> postinglists) {
        int[] docIds = new int[postinglists.size()];
        // System.out.println("current id is: "+docId);
        int previousVal = -1;
        boolean equilibrium = true;
        int maximum = Integer.MIN_VALUE;
        for (int i = 0; i < postinglists.size(); i++) {
            int currentId = nextInOccurence(docId, postinglists.get(i));
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
            return nextForOccurence(maximum - 1, postinglists);
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

            // int curr_id = decompressBytes(code);
            // if ( curr_id != last_id){
            // last_id = curr_id;
            // ++count;
            // }
            // skip the occurrence number
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

    /**
     * @CS2580: Implement this for bonus points.
     */

    // do linear search of a docid for compressed posting list, first occurrence
    private int linearSearchPostList(final int docId, final List<Byte> list) {
        int i = 0;
        int pos = -1;
        while (i < list.size()) {
            pos = i;
            ArrayList<Byte> code = new ArrayList<Byte>();
            byte currByte = list.get(i);
            while ((currByte & 0x80) == (byte) 0) {
                code.add(currByte);
                i++;
                if (i >= list.size()) {
                    return -1;
                } else {
                    currByte = list.get(i);
                }
            }
            code.add(currByte);
            i++;

            if (decompressBytes(code) == docId) {
                return pos;
            }

            if (i >= list.size()) {
                return -1;
            }
            while ((list.get(i) & 0x80) == (byte) 0) {
                i++;
                if (i >= list.size()) {
                    return -1;
                }
            }
            i++;
        }
        return -1;
    }

    @Override
    public int documentTermFrequency(String term, String url) {
        // Get docid for specific url.
        int docid = docUrlMap.get(url);

        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());
        s.stem();

        if (!ivtContainsKey(s.toString())) {
            return 0;
        }

        // Get posting list from index.
        List<Byte> l = ivtGet(s.toString());

        // Use binary search looking for docid within given posting list.
        int pos = linearSearchPostList(docid, l);

        if (pos != -1) {
            // Return term frequency for given doc and term
            int count = 0;
            while (pos < l.size() - 1) {
                ArrayList<Byte> currId = new ArrayList<Byte>();
                // get the current id
                while ((l.get(pos) & 0x80) == (byte) 0) {
                    currId.add(l.get(pos));
                    pos++;
                }
                currId.add(l.get(pos));
                pos++;

                int curr_id = decompressBytes(currId);
                if (curr_id == docid) {
                    ++count;
                    pos++;
                } else {
                    break;
                }
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
        ArrayList<Byte> codes = new ArrayList<Byte>();
        ArrayList<Byte> a1 = compressInt(1000234567);
        codes.addAll(a1);
        ArrayList<Byte> a2 = compressInt(1000);
        codes.addAll(a2);
        ArrayList<Byte> a3 = compressInt(67);
        codes.addAll(a3);
        ArrayList<Byte> a4 = compressInt(10002345);
        codes.addAll(a4);
        ArrayList<Integer> b = decompressArray(codes);
        System.out.println(codes + "->" + b);
    }
}
