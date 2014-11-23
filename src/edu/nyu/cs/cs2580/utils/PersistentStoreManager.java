
package edu.nyu.cs.cs2580.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.MapSerializer;
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;

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

    public static class IvtMapByte implements Map<String, List<Byte>> {
        private Map<String, Integer> ivtOffset;
        private DataOutputStream postListOut;
        private RandomAccessFile postListIn;
        private File idxFile;
        private boolean isNew;

        public IvtMapByte(File dir, String mapName, boolean isNew) {
            this.isNew = isNew;
            idxFile = new File(dir, mapName + ".idx");
            File file = new File(dir, mapName);
            if (isNew) {
                createNewMap(file, mapName);
            } else {
                loadMap(file, mapName);
            }
        }

        private void loadMap(File file, String mapName) {
            try {
                postListIn = new RandomAccessFile(file, "r");

                FileInputStream streamIn = new FileInputStream(idxFile);
                ObjectInputStream objectinputstream = new ObjectInputStream(streamIn);
                ivtOffset = (Map<String, Integer>) objectinputstream.readObject();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        private void createNewMap(File file, String mapName) {
            try {
                postListOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(
                        file, false)));
                postListIn = new RandomAccessFile(file, "r");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            ivtOffset = new HashMap<String, Integer>();
        }

        @Override
        public void clear() {
        }

        @Override
        public boolean containsKey(Object key) {
            return ivtOffset.containsKey(key);
        }

        @Override
        public boolean containsValue(Object value) {
            return false;
        }

        @Override
        public Set<java.util.Map.Entry<String, List<Byte>>> entrySet() {
            return null;
        }

        @Override
        public List<Byte> get(Object key) {
            int offset = ivtOffset.get(key);
            List<Byte> result = new ArrayList<Byte>();
            try {
                postListIn.seek(offset);
                int size = postListIn.readInt();
                for (int i = 0; i < size; i++) {
                    result.add(postListIn.readByte());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }

        @Override
        public boolean isEmpty() {
            return ivtOffset.isEmpty();
        }

        @Override
        public Set<String> keySet() {
            return ivtOffset.keySet();
        }

        @Override
        synchronized public List<Byte> put(String key, List<Byte> value) {
            int offset = postListOut.size();
            int size = value.size();
            ivtOffset.put(key, offset);
            try {
                postListOut.writeInt(size);
                for (byte b : value) {
                    postListOut.writeByte(b);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return value;
        }

        @Override
        public void putAll(Map<? extends String, ? extends List<Byte>> m) {
            for (String key : m.keySet()) {
                this.put(key, m.get(key));
            }
        }

        @Override
        public List<Byte> remove(Object key) {
            return null;
        }

        @Override
        public int size() {
            return ivtOffset.size();
        }

        @Override
        public Collection<List<Byte>> values() {
            return null;
        }

        public void close() {
            try {
                if (isNew) {
                    storeIndex();
                    postListOut.flush();
                    postListOut.close();
                } else {
                    postListIn.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void storeIndex() {
            try {
                FileOutputStream fout = new FileOutputStream(idxFile);
                ObjectOutputStream oos = new ObjectOutputStream(fout);
                oos.writeObject(ivtOffset);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void finalize() throws Throwable {
            super.finalize();
            this.close();
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
                Input in = new Input(new BufferedInputStream(new FileInputStream(
                        raf.getFD())));
                result = kryo.readObject(in, HashMap.class);
                in.close();
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
