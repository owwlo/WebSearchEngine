
package edu.nyu.cs.cs2580.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mapdb.DB;
import org.mapdb.DBMaker;

import edu.nyu.cs.cs2580.SearchEngine.Options;

public class AssistantIndexBuilder {
    private static final String DEFAULT_PATH = "./assistant_indexes/";
    private static final String DB_FILE = "assistant";

    private static final String TWO_LETTER_DB = "2l";
    private static final String TERM_LEN_DB = "tl";
    private static final String TERM_POSITION_DB = "termPosition";

    private static AssistantIndexBuilder instance = null;

    Map<String, List<String>> twoLetterMap;
    Map<Integer, List<String>> termLenMap;
    Map<Integer, List<String>> termPositionMap;

    private Options options = null;

    private DB db = null;

    public static interface ITermPosition {
        public String getTermAtPosition(int docId, int position);
    }

    public static interface ILetterTermList {
        public List<String> getLetterTermList(String letters);
    }

    public static interface ITermLenList {
        public List<String> getTermLenList(int len);
    }

    public static interface ICacheKeeper {
        public List<String> getCachedPage(int docid);
    }

    private AssistantIndexBuilder(Options opts) {
        options = opts;
        File dbFile = new File(options._indexPrefix + "/../", DB_FILE);
        db = DBMaker.newFileDB(dbFile)
                .transactionDisable()
                .closeOnJvmShutdown()
                .make();
        twoLetterMap = db.createHashMap(TWO_LETTER_DB)
                .makeOrGet();
        termLenMap = db.createHashMap(TERM_LEN_DB)
                .makeOrGet();
        termPositionMap = db.createHashMap(TERM_POSITION_DB)
                .makeOrGet();
    }

    public ITermPosition getTermPositionManager() {
        final Map<Integer, List<String>> termPositionMap = db.createHashMap(TERM_POSITION_DB)
                .makeOrGet();
        return new ITermPosition() {

            @Override
            public String getTermAtPosition(int docId, int position) {
                List<String> lst = termPositionMap.get(docId);
                if (lst != null && position < lst.size()) {
                    return lst.get(position);
                }
                return null;
            }
        };
    }

    public ICacheKeeper getCacheKeeper() {
        final Map<Integer, List<String>> termPositionMap = db.createHashMap(TERM_POSITION_DB)
                .makeOrGet();
        return new ICacheKeeper() {

            @Override
            public List<String> getCachedPage(int docid) {
                return termPositionMap.get(docid);
            }
        };
    }

    public ILetterTermList getTwoLetterTermListManager() {
        final Map<String, List<String>> twoLetterMap = db.createHashMap(TWO_LETTER_DB)
                .makeOrGet();
        return new ILetterTermList() {

            @Override
            public List<String> getLetterTermList(String letters) {
                return twoLetterMap.get(letters);
            }
        };
    }

    public ITermLenList getTermLenListManager() {
        final Map<Integer, List<String>> termLenMap = db.createHashMap(TERM_LEN_DB)
                .makeOrGet();
        return new ITermLenList() {

            @Override
            public List<String> getTermLenList(int len) {
                return termLenMap.get(len);
            }
        };
    }

    public void buildTwoLetterMap(Set<String> terms) {
        if (twoLetterMap.size() == 0) {
            Map<String, List<String>> tmpMap = new HashMap<String, List<String>>();
            for (String word : terms) {
                for (int i = 0; i < word.length() - 1; ++i) {
                    String twoLetters = word.substring(i, i + 2);
                    if (!tmpMap.containsKey(twoLetters)) {
                        tmpMap.put(twoLetters, new ArrayList<String>());
                    }
                    List<String> newList = tmpMap.get(twoLetters);
                    newList.add(word);
                    tmpMap.put(twoLetters, newList);
                }
            }
            twoLetterMap.putAll(tmpMap);
            db.commit();
            db.compact();
        }
    }

    public void buildDocTermPositionMap(int docId, List<String> list) {
        if (termPositionMap.size() % 500 == 0) {
            db.commit();
            db.compact();
        }
        termPositionMap.put(docId, list);
    }

    public void buildTermLenMap(Set<String> terms) {
        if (termLenMap.size() == 0) {
            Map<Integer, List<String>> tmpMap = new HashMap<Integer, List<String>>();
            for (String word : terms) {
                int len = word.length();
                if (!tmpMap.containsKey(len)) {
                    tmpMap.put(len, new ArrayList<String>());
                }
                List<String> newList = tmpMap.get(len);
                newList.add(word);
                tmpMap.put(len, newList);
            }
            termLenMap.putAll(tmpMap);
            db.commit();
            db.compact();
        }
    }

    public static AssistantIndexBuilder getInstance(Options opts) {
        if (instance == null) {
            instance = new AssistantIndexBuilder(opts);
        }
        return instance;
    }

    public void close() {
        db.commit();
        db.compact();
        db.close();
    }

    public static void cleanFiles(Options opts) {
        File dir = new File(opts._indexPrefix + "/../");
        if (dir.exists()) {
            for (File file : dir.listFiles()) {
                if (file.getName().startsWith(DB_FILE)) {
                    file.delete();
                }
            }
        }
    }
}
