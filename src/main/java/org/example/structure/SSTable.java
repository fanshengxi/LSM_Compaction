package org.example.structure;

import java.util.ArrayList;

public class SSTable {
    int FileSize;//文件大小
    int time; //时间
    ArrayList<KVPair> pairs; //一组键值对成员
    int nKeys;//文件中的键值对数量

    public int getnKeys() {
        return nKeys;
    }

    public void setnKeys(int nKeys) {
        this.nKeys = nKeys;
    }

    public int getFileSize() {
        return FileSize;
    }

    public void setFileSize(int fileSize) {
        FileSize = fileSize;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    public ArrayList<KVPair> getPairs() {
        return pairs;
    }

    public void setPairs(ArrayList<KVPair> pairs) {
        this.pairs = pairs;
    }
}
