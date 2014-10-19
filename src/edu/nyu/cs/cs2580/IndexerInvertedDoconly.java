
package edu.nyu.cs.cs2580;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.jsoup.Jsoup;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import edu.nyu.cs.cs2580.SearchEngine.Options;

/**
 * @CS2580: Implement this class for HW2.
 */
public class IndexerInvertedDoconly extends Indexer {
    private DB documentDB = null;

    private Map<Integer, DocumentIndexed> docMap = null;
    private Map<String, List<Integer>> docInvertedMap = null;
    private Map<String, Integer> docUrlMap = null;

    // Table name for index of documents.
    private static final String DOC_IDX_TBL = "docDB";
    private static final String DOC_IVT_TBL = "docIvtDB";
    private static final String DOC_URL_TBL = "docUrlDB";

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

    @Override
    public void constructIndex() throws IOException {
        String corpusFolder = _options._corpusPrefix;
        System.out.println("Construct index from: " + corpusFolder);

        long start_t = System.currentTimeMillis();

        cleanUpDirectory();

        // Get all corpus files.
        List<File> files = getAllFiles(new File(corpusFolder));

        initialStore(false);

        Map<String, List<Integer>> ivtMap = new HashMap<String, List<Integer>>();

        System.out.println("Start building index for index. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

        int docId = 0;
        for (File file : files) {
            Map<String, Integer> ivtMapItem = new HashMap<String, Integer>();

            String htmlStr = FileUtils.readFileToString(file);
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
                    ivtMapItem.put(token, 0);
                }
                ivtMapItem.put(token, ivtMapItem.get(token) + 1);
                passageLength++;
            }

            String url = file.getCanonicalPath();

            DocumentIndexed di = new DocumentIndexed(docId);
            di.setTitle(title);
            di.setUrl(url);
            di.setLength(passageLength);

            for (String token : ivtMapItem.keySet()) {
                if (!ivtMap.containsKey(token)) {
                    ivtMap.put(token, new ArrayList<Integer>());
                }
                List<Integer> recordList = ivtMap.get(token);
                recordList.add(docId);
                recordList.add(ivtMapItem.get(token));
            }

            buildDocumentIndex(di);

            docId++;
        }

        // Write ivtMap into storage.
        long recordsCommit = 0;
        System.out.println("Writing Inverted Map to disk. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");
        for (String token : ivtMap.keySet()) {
            if (recordsCommit % 200000 == 0) {
                documentDB.commit();
                System.out.println("Records commit size: " + recordsCommit);
            }

            List<Integer> ivtRecordList = ivtMap.get(token);
            docInvertedMap.put(token, ivtRecordList);

            recordsCommit++;
        }
        documentDB.commit();
        documentDB.compact();
        documentDB.close();

        long end_t = System.currentTimeMillis();

        System.out.println("Construct done. Duration: " + (end_t - start_t) / 1000.0 + "s");
    }

    private void initialStore(boolean readOnly) {
        // Initialize Database
        File f = new File(_options._indexPrefix, "docIdx");

        if (readOnly) {
            documentDB = DBMaker.newFileDB(f)
                    .mmapFileEnable()
                    .readOnly()
                    .transactionDisable()
                    .compressionEnable()
                    .make();
        } else {
            documentDB = DBMaker.newFileDB(f)
                    .mmapFileEnable()
                    .transactionDisable()
                    .asyncWriteEnable()
                    .compressionEnable()
                    .make();
        }

        docMap = documentDB.createHashMap(DOC_IDX_TBL)
                .makeOrGet();
        docInvertedMap = documentDB.createHashMap(DOC_IVT_TBL)
                .makeOrGet();
        docUrlMap = documentDB.createHashMap(DOC_URL_TBL)
                .makeOrGet();
    }

    private void cleanUpDirectory() {
        File dir = new File(_options._indexPrefix);
        dir.mkdirs();
        for (File file : dir.listFiles()) {
            file.delete();
        }
    }

    private void buildDocumentIndex(DocumentIndexed di) {
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
        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());

        if (!docInvertedMap.containsKey(s.toString())) {
            return 0;
        }

        // Get posting list from index.
        List<Integer> l = docInvertedMap.get(s.toString());

        return l.size() / 2;
    }

    @Override
    public int corpusTermFrequency(String term) {
        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());

        if (!docInvertedMap.containsKey(s.toString())) {
            return 0;
        }

        // Get posting list from index.
        List<Integer> l = docInvertedMap.get(s.toString());

        int result = 0;
        for (int i = 0; i < l.size() / 2; i++) {
            result += l.get(i * 2 + 1);
        }

        return result;
    }

    private int bsInner(final int start, final int end, final int docid, final List<Integer> list) {
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
        return chk;
    }

    private int binarySearchPostList(final int docId, final List<Integer> list) {
        return bsInner(0, list.size() - 1, docId, list);
    }

    @Override
    public int documentTermFrequency(String term, String url) {
        // Get docid for specific url.
        int docid = docUrlMap.get(url);

        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());

        if (!docInvertedMap.containsKey(s.toString())) {
            return 0;
        }

        // Get posting list from index.
        List<Integer> l = docInvertedMap.get(s.toString());

        // Use binary search looking for docid within given posting list.
        int pos = binarySearchPostList(docid, l);

        if (pos != -1) {
            // Return term frequency for given doc and term
            return l.get(pos + 1);
        } else {
            return 0;
        }
    }
}
