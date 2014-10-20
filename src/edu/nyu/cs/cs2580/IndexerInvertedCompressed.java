
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
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
public class IndexerInvertedCompressed extends Indexer {
    private DB documentDB = null;

    private ArrayList<DB> ivtIndexDBList = new ArrayList<DB>();
    private ArrayList<Map<String, List<Byte>>> ivtIndexMapList = new ArrayList<Map<String, List<Byte>>>();

    // for compressed posting list
    private Map<String, List<Byte>> compressedPL = new HashMap<String, List<Byte>>();
    // for record in query
    private String currTerm; // current query term
    private Map<Integer, Integer> currPL; // current posting list: <docid,
    // counts>

    private Map<Integer, DocumentIndexed> docMap = null;
    private Map<String, Integer> docUrlMap = null;

    // Table name for index of documents.
    private static final String DOC_IDX_TBL = "docDB";
    private static final String DOC_IVT_TBL = "docIvtDB";
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

        public InvertIndexBuildingTask(List<File> files, int startFileIdx,
                int endFileIdx, Map<String, List<Byte>> ivtMap) {
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
                    // for each token in each doc, add <token, occurrence> to
                    // the
                    // map of <token, {occurrence list}> for each document
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

                // for each token in each document, add to the map of <token,
                // {docid, occ}>
                for (String token : ivtMapItem.keySet()) {
                    if (!ivtMap.containsKey(token)) {
                        ivtMap.put(token, new ArrayList<Byte>());
                    }
                    List<Byte> recordList = ivtMap.get(token);

                    ArrayList<Integer> occList = ivtMapItem.get(token);
                    ArrayList<Byte> _docId = IndexerInvertedCompressed
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

            Map<String, List<Byte>> ivtMap = new HashMap<String, List<Byte>>();

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
            Map<String, List<Byte>> ivtMapPerDB = ivtIndexMapList.get(batchNum);

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
            Map<String, List<Byte>> hm = d.createHashMap(DOC_IVT_TBL)
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
     * In HW2, you should be using {@link DocumentIndexed}
     */
    @Override
    public Document nextDoc(Query query, int docid) {
        return null;
    }

    @Override
    public int corpusDocFrequencyByTerm(String term) {
        // Number of documents in which {@code term} appeared, over the full
        // corpus.

        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());

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
                currByte = list.get(i++);
            }
            code.add(currByte);
            i++;

            if (decompressBytes(code) == docId) {
                return pos;
            }

            while ((list.get(i) & 0x80) == (byte) 0) {
                i++;
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

    private List<Byte> ivtGet(String key) {
        List<Byte> l = new ArrayList<Byte>();
        for (Map<String, List<Byte>> m : ivtIndexMapList) {
            if (m.containsKey(key)) {
                l.addAll(m.get(key));
            }
        }
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
        ArrayList<Byte> a = compressInt(1000234567);
        int b = decompressBytes(a);
        System.out.println(a + "->" + b);
    }
}
