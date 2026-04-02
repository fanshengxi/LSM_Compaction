package org.example.structure;

public class KVPair {
    int key; //32位有符号整数
    String value; //字符串（仅包含大小写字母和数字，且长度可以为0）

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
