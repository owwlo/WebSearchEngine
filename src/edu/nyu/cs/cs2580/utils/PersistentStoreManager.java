
package edu.nyu.cs.cs2580.utils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class PersistentStoreManager {

    public static Object readObjectFromFile(File file) {
        Object obj = null;
        try {
            FileInputStream streamIn = new FileInputStream(file);
            ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
            obj = objectinputstream.readObject();
            objectinputstream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return obj;
    }

    public static void writeObjectToFile(File file, Object obj) {
        try {
            FileOutputStream fout = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fout);
            oos.writeObject(obj);
            oos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class TermFrequencyManager {
        private final String IDX_FILE_NAME = "tfm.idx";
        private final String DAT_FILE_NAME = "tfm.dat";

        private Map<Integer, Integer> offsetMap;

        private Output datOut;
        private RandomAccessFile raf;

        private File idxFile;
        private File datFile;

        private Kryo kryo = new Kryo();

        public TermFrequencyManager(String basePath) {
            idxFile = new File(basePath, IDX_FILE_NAME);
            datFile = new File(basePath, DAT_FILE_NAME);
            if (idxFile.exists()) {
                offsetMap = (Map<Integer, Integer>) readObjectFromFile(idxFile);
                try {
                    raf = new RandomAccessFile(datFile, "r");
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } else {
                offsetMap = new HashMap<Integer, Integer>();
                try {
                    datOut = new Output(new BufferedOutputStream(
                            new FileOutputStream(datFile, false)));
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        public void addTermFrequencyForDoc(int docId, Map<String, Integer> frequencyMap) {
            int offset = (int) datOut.total();
            kryo.writeObject(datOut, frequencyMap);
            offsetMap.put(docId, offset);
        }

        public Map<String, Integer> gettermFrequencyForDoc(int docid) {
            int offset = offsetMap.get(docid);
            Map<String, Integer> result = null;
            try {
                raf.seek(offset);
                result = kryo.readObject(new Input(new FileInputStream(
                        raf.getFD())), HashMap.class);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return result;
        }

        public void close() {
            writeObjectToFile(idxFile, offsetMap);
            if (datOut != null) {
                datOut.close();
            }
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                }
            }
        }
    }
}
