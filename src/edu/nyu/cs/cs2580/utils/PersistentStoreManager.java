
package edu.nyu.cs.cs2580.utils;

import java.io.BufferedOutputStream;
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

    public static class IvtMap implements Map<String, List<Integer>> {
        private Map<String, Integer> ivtOffset;
        private DataOutputStream postListOut;
        private RandomAccessFile postListIn;
        private File idxFile;
        private boolean isNew;

        public IvtMap(File dir, String mapName, boolean isNew) {
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
                postListOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file, false)));
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
        public Set<java.util.Map.Entry<String, List<Integer>>> entrySet() {
            return null;
        }

        @Override
        public List<Integer> get(Object key) {
            int offset = ivtOffset.get(key);
            List<Integer> result = new ArrayList<Integer>();
            try {
                postListIn.seek(offset);
                int size = postListIn.readInt();
                for (int i = 0; i < size; i++) {
                    result.add(postListIn.readInt());
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
        synchronized public List<Integer> put(String key, List<Integer> value) {
            int offset = postListOut.size();
            int size = value.size();
            ivtOffset.put(key, offset);
            try {
                postListOut.writeInt(size);
                for (int integer : value) {
                    postListOut.writeInt(integer);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return value;
        }

        @Override
        public void putAll(Map<? extends String, ? extends List<Integer>> m) {
            for (String key : m.keySet()) {
                this.put(key, m.get(key));
            }
        }

        @Override
        public List<Integer> remove(Object key) {
            return null;
        }

        @Override
        public int size() {
            return ivtOffset.size();
        }

        @Override
        public Collection<List<Integer>> values() {
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
}