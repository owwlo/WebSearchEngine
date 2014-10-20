
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
                Map<String, Integer> ivtMapItem = new HashMap<String, Integer>();

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
                        ivtMapItem.put(token, 0);
                    }
                    ivtMapItem.put(token, ivtMapItem.get(token) + 1);
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

        initialStore(false);

        int threadCount = 1;

        System.out.println("Start building index with " + threadCount + " threads. Elapsed: "
                + (System.currentTimeMillis() - start_t) / 1000.0 + "s");

        int filesPerBatch = 2000;
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
     * In HW2, you should be using {@link DocumentIndexed}
     */
    
    
    
   private static int nextForOne(int docId,List<Integer> postinglists){
    	if (postinglists==null)
    		return -1;
        if (postinglists.size()<1||postinglists.get(postinglists.size()-2)<=docId)	
    	  return -1;
    	if (postinglists.get(0)>docId)
    		return postinglists.get(0);
    	int start=0;
    	int end=postinglists.size()/2-1;
    	while (end-start>1){
    		int mid=(start+end)/2;
    		if (postinglists.get(mid*2)<=docId)
    			start=mid;
    		else
    			end=mid;
    	}
    	return postinglists.get(2*end);
    }     
    
    
    
    private static int next(int docId,Vector<List<Integer>> postinglists){
    	int[] docIds=new int[postinglists.size()];
    	//System.out.println("current id is: "+docId);
    	int previousVal=-1;
        boolean equilibrium=true;
        int maximum=Integer.MIN_VALUE;
    	for (int i=0;i<postinglists.size();i++){
    		int currentId=nextForOne(docId,postinglists.get(i));
    		if (currentId<0)
    			return -1;
    		if (previousVal<0){
    			previousVal=currentId;
    			maximum=currentId;
    		}
    		else {
    			if (previousVal!=currentId){
    				equilibrium=false;
    				maximum=Math.max(maximum, currentId);
    			}
    		}   		
    	}
    	if (equilibrium==true)
    		return previousVal;
    	else
    		return next(maximum-1,postinglists);
    }
    @Override
    public Document nextDoc(Query query, int docid) {
    	//query.processQuery();
    	Vector<String> tokens=query._tokens;
        int result=-1;
    	Vector<List<Integer>> postingLists=new Vector<List<Integer>> ();
    	for (int i=0;i<tokens.size();i++){
            Stemmer s = new Stemmer();
            s.add(tokens.get(i).toLowerCase().toCharArray(), tokens.get(i).length());
            s.stem();
            //System.out.println("size is: "+docInvertedMap.get(s.toString()).size());
    		postingLists.add(docInvertedMap.get(s.toString()));
    	}
        result=next(docid,postingLists);
        //System.out.println("the result is:"+result);
    	if (result<0)
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
        s.stem();
        //System.out.println("term is: "+ term+ "stemmer is: "+s.toString());
        if (!docInvertedMap.containsKey(s.toString())) {
        	//System.out.println("it is not contained...");
            return 0;
        }
        //System.out.println("OMG....");
        // Get posting list from index.
        List<Integer> l = docInvertedMap.get(s.toString());

        int result = 0;
        for (int i = 0; i < l.size() / 2; i++) {
            result += l.get(i * 2 + 1);
        }

        return result;
    }

    private static int bsInner(final int start, final int end, final int docid, final List<Integer> list) {
        /*if (end - start <= 1) {
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
        }*/
    	int Start=start;
    	int End=end;
    	while (Start<=End){
    		int mid=(Start+End)/2;
    		if (docid==list.get(2*mid))
    			return (2*mid);
    		if (docid<list.get(2*mid))
    			End=mid-1;
    		else
    			Start=mid+1;
    		//System.out.println("Start is: "+Start+"End is :"+End);
    	}
        return -1;
    }

    private int binarySearchPostList(final int docId, final List<Integer> list) {
       // return bsInner(0, list.size() - 1, docId, list);
    	//Modification:
    	return bsInner(0,list.size()/2-1,docId,list);
    }

    @Override
    public int documentTermFrequency(String term, String url) {
        // Get docid for specific url.
        int docid = docUrlMap.get(url);
        
        // Stem given term.
        Stemmer s = new Stemmer();
        s.add(term.toLowerCase().toCharArray(), term.length());
        s.stem();

        if (!docInvertedMap.containsKey(s.toString())) {
            return 0;
        }
        // Get posting list from index.
        List<Integer> l = docInvertedMap.get(s.toString());

        // Use binary search looking for docid within given posting list.
        int pos = binarySearchPostList(docid, l);
      /*  if (docid==637){
        	System.out.println("i am at 637");
        	System.out.println("position is:"+l.get(pos+1));
        	for (int i=0;i<l.size();i++)
        		System.out.printf(" "+l.get(i)+" ");
        }*/
        if (pos != -1) {
            // Return term frequency for given doc and term
       // 	System.out.println("current num is: "+l.get(pos+1));
            return l.get(pos + 1);
        } else {
            return 0;
        }
    }
    public static void main(String[] args){
    	/*int[] first={1,1,2,1,5,1,7,1,8,1};
    	int[] second={3,1,4,1,8,1};
    	int[] third={8,2,9,3};
    	ArrayList<Integer> firstA=new ArrayList<Integer> ();
    	ArrayList<Integer> secondA=new ArrayList<Integer> ();
    	ArrayList<Integer> thirdA=new ArrayList<Integer> ();
    	for (int i=0;i<first.length;i++)
    		firstA.add(first[i]);
    	for (int i=0;i<second.length;i++)
    		secondA.add(second[i]);
    	for (int i=0;i<third.length;i++)
    		thirdA.add(third[i]);
    	Vector<List<Integer>> posting=new Vector<List<Integer>> ();
    	posting.add(firstA);
    	posting.add(secondA);
    	posting.add(thirdA);
    	int k=next(-1,posting);
    	//int k=nextForOne(7,firstA);
    	System.out.println(k);*/
    	int docId=8;
    	int[] first={1,1,2,1,5,4,7,1,8,1};
    	ArrayList<Integer> firstA=new ArrayList<Integer> ();
    	for (int i=0;i<first.length;i++)
    		firstA.add(first[i]);
    	System.out.println("result is: "+bsInner(0,firstA.size()/2-1,docId, firstA));
    }
    
}
