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

  private Map<Integer, DocumentIndexed> docMap = null;
  private Map<String, List<Integer>> docInvertedMap = null;
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
      private Map<String, Queue<Integer>> ivtMap;

      public InvertIndexBuildingTask(List<File> files, int startFileIdx, int endFileIdx,
              Map<String, Queue<Integer>> ivtMap) {
          this.files = files;
          this.startFileIdx = startFileIdx;
          this.endFileIdx = endFileIdx;
          this.ivtMap = ivtMap;
      }

      @Override
      public void run() {
          System.out.println("Thread " + Thread.currentThread().getName()
                  + " processes files from " + startFileIdx
                  + " to " + endFileIdx);
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
                      ivtMap.put(token, new ConcurrentLinkedQueue<Integer>());
                  }
                  Queue<Integer> recordList = ivtMap.get(token);
                  
                  ArrayList<Integer> occList = ivtMapItem.get(token);
                  // sequentially add <docid, occurrence> to the posting list.
                  for (int e : occList){
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

    initialStore(false);

    int threadCount = Runtime.getRuntime().availableProcessors();

    System.out.println("Start building index with " + threadCount + " threads. Elapsed: "
            + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

    int filesPerBatch = 1000;
    for (int batchNum = 0; batchNum < files.size() / filesPerBatch; batchNum++) {
        int fileIdStart = batchNum * filesPerBatch;
        int fileIdEnd = (batchNum + 1) * filesPerBatch;
        if (batchNum == (files.size() / filesPerBatch) - 1) {
            fileIdEnd = files.size();
        }

        System.out.println("Processing files from " + fileIdStart + " to " + fileIdEnd);

        ExecutorService threadPool = Executors.newFixedThreadPool(threadCount);

        Map<String, Queue<Integer>> ivtMap = new ConcurrentHashMap<String, Queue<Integer>>();

        int totalFileCount = fileIdEnd - fileIdStart;
        int filesPerThread = totalFileCount / threadCount;
        for (int threadId = 0; threadId < threadCount; threadId++) {
            int startFileIdx = threadId * filesPerThread + fileIdStart;
            int endFileIdx = (threadId + 1) * filesPerThread + fileIdStart;
            if (threadId == threadCount - 1) {
                endFileIdx = fileIdEnd;
            }
            InvertIndexBuildingTask iibt = new InvertIndexBuildingTask(files, startFileIdx,
                    endFileIdx, ivtMap);
            threadPool.submit(iibt);
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println(fileIdEnd + " pages have been processed. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");
        // Write ivtMap into storage.
        long recordsCommit = 0;
        System.out.println("Writing Inverted Map to disk. " + fileIdEnd
                + " pages have been processed. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");
        for (String token : ivtMap.keySet()) {
            if (recordsCommit % 200000 == 0 && recordsCommit != 0) {
                documentDB.commit();
                System.out.println("Records commit size: " + recordsCommit);
            }
            List<Integer> ivtRecordList = new ArrayList<Integer>(ivtMap.get(token));
            if (docInvertedMap.containsKey(token)) {
                List<Integer> dbRecordList = docInvertedMap.get(token);
                dbRecordList.addAll(ivtRecordList);
                docInvertedMap.put(token, dbRecordList);
            } else {
                docInvertedMap.put(token, ivtRecordList);
            }

            recordsCommit++;
        }
        documentDB.commit();
        System.out.println("Batch commit done.");
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
