package org.example;

import org.example.structure.KVPair;
import org.example.structure.SSTable;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Main {
    static ArrayList<SSTable> ssTables;
    static ArrayList<KVPair> sortedKVPairs,cleanKVPairs;
    static String path = "../small-case";
    public static void main(String[] args) throws IOException {
        loadSSTables();

        sortSSTables();

        cleanSSTables();

        saveSSTables();
    }

    //步骤1
    static void loadSSTables() throws IOException {
        ssTables = new ArrayList<>();

        File small_dir = new File(path);

        File[] files = small_dir.listFiles();
        if (files==null) return;
        if (files.length==0) return;
        for (int i=0; i<files.length;i++) {
            if (!files[i].getName().contains("sstable-")) continue;

            SSTable ssTable = new SSTable();
            ArrayList<KVPair> kvPairs = new ArrayList<>();

            String filePath = files[i].getPath();
            Path path = Paths.get(filePath);
            byte[] bytes = Files.readAllBytes(path);
            String fileSize = "",time = "",nKeys="";
            int lastKey=0,lastOffset=0,curKey=0,curOffset=0;
            StringBuilder lastValue= new StringBuilder();

            int j = 0;
            for (j = 0; j < bytes.length; j++) {

                //获取文件大小（前4个字节）
                if (j<4){
                    String bCode = String.format("%8s",Integer.toBinaryString(bytes[j]&0xff)).replace(' ','0');
                    fileSize = bCode.concat(fileSize);
                    continue;
                }
                if (j==4) ssTable.setFileSize(Integer.parseInt(fileSize,2));
                //获取文件被生成的时间（数字越大，文件生成的时间越晚）
                if (j<8){
                    String bCode = String.format("%8s",Integer.toBinaryString(bytes[j]&0xff)).replace(' ','0');
                    time = bCode.concat(time);
                    continue;
                }
                if (j==8) ssTable.setTime(Integer.parseInt(time,2));
                //文件中的键值对数量
                if (j<12){
                    String bCode = String.format("%8s",Integer.toBinaryString(bytes[j]&0xff)).replace(' ','0');
                    nKeys = bCode.concat(nKeys);
                    continue;
                }
                if (j==12) ssTable.setnKeys(Integer.parseInt(nKeys,2));
                break;
            }

            int max = Integer.MIN_VALUE,min = Integer.MAX_VALUE;
            for (int k = 0; k < ssTable.getnKeys(); k++) {
                String bKey="";//键
                String bOffset = "";//偏移量
                for (int l = 0; l < 4; l++) {
                    int offset = j+k*8+l;

                    String bCode = String.format("%8s",Integer.toBinaryString(bytes[offset]&0xff)).replace(' ','0');
                    bKey = bCode.concat(bKey);
                }

                for (int l = 0; l < 4; l++) {
                    int offset = j+k*8+4+l;

                    String bCode = String.format("%8s",Integer.toBinaryString(bytes[offset]&0xff)).replace(' ','0');
                    bOffset = bCode.concat(bOffset);
                }

                if (k>0) {
                    lastKey = curKey;
                    lastOffset = curOffset;
                }
                curKey = Integer.parseInt(bKey,2);
                if (curKey>max) max = curKey;
                if (curKey<min) min = curKey;
                curOffset = Integer.parseInt(bOffset,2);

                if (k>0){
                    for (int l = lastOffset; l < curOffset; l++) {
                        lastValue.append(((char) bytes[l]));
                    }

                    KVPair kvPair = new KVPair();
                    kvPair.setKey(lastKey);
                    kvPair.setValue(lastValue.toString());
                    kvPairs.add(kvPair);
                }
                lastValue = new StringBuilder();

            }
            //处理最后一个键对应的值
            for (int l = lastOffset; l < ssTable.getFileSize(); l++) {
                lastValue.append(((char) bytes[l]));
            }

            KVPair kvPair = new KVPair();
            kvPair.setKey(curKey);
            kvPair.setValue(lastValue.toString());
            kvPairs.add(kvPair);

            ssTable.setPairs(kvPairs);
            ssTables.add(ssTable);

            System.out.println(ssTable.getnKeys() + " " + min + " " + max);//输出键值对个数
        }
    }

    //步骤2
    static void sortSSTables() {
        sortedKVPairs = new ArrayList<>();

        int num = ssTables.size();

        int[] index = new int[num];
        int[] keys = new int[num];

        for (int i = 0; i < num; i++) {
            keys[i] = ssTables.get(i).getnKeys();
        }

        while (true){
            int minTable = 0;//最小key的表索引
            KVPair minPair = null;

            for (int i = 0; i <= num; i++) {

                if (i==num) {
                    sortedKVPairs.add(minPair);
                    index[minTable]++;
                    break;
                }

                if (minPair==null){
                    if (index[i]<keys[i]) {
                        minPair = ssTables.get(i).getPairs().get(index[i]);
                        minTable = i;
                    }
                    continue;
                }
                //未归并完，需找到最小值
                if (index[i]<keys[i]) {
                    KVPair curPair = ssTables.get(i).getPairs().get(index[i]);
                    if (curPair.getKey()<minPair.getKey() ||
                            (curPair.getKey()==minPair.getKey() && ssTables.get(i).getTime()<ssTables.get(minTable).getTime())){
                        minPair = curPair;
                        minTable = i;
                    }
                }


            }
            boolean finished = true;
            for (int i = 0; i < num; i++) {
                finished &= index[i]==keys[i];
            }

            if (finished)
                break;
        }

        System.out.println(sortedKVPairs.get(0).getKey() + " " + sortedKVPairs.get(sortedKVPairs.size()-1).getKey());

    }

    //步骤3
    static void cleanSSTables() {
        cleanKVPairs = new ArrayList<>();


        for (int i = 0; i < sortedKVPairs.size(); i++) {
            int j = i+1;
            while (j < sortedKVPairs.size() && sortedKVPairs.get(i).getKey()==sortedKVPairs.get(j).getKey())
                j++;

            j-=1;

            if (i>j) break;

            if (!sortedKVPairs.get(j).getValue().isEmpty()) {
                cleanKVPairs.add(sortedKVPairs.get(j));
            }

            i=j;
        }

        System.out.println(cleanKVPairs.size() + " " +cleanKVPairs.get(0).getKey() + " " + cleanKVPairs.get(cleanKVPairs.size()-1).getKey());
    }

    //步骤4
    static void saveSSTables() throws IOException {
        int fileNum = 0; // 输出时要+1
        int time = 0x00ffffff,idx=4,nKeys=0,curValueSize=0;

        ByteBuffer buffer = ByteBuffer.allocate(262144).order(ByteOrder.LITTLE_ENDIAN);//小端方式

        buffer.putInt(idx,time);//存放时间
        idx+=8;

        for (int i = 0; i < cleanKVPairs.size(); i++) {
            if ((nKeys+1)*8+3*4+curValueSize+cleanKVPairs.get(i).getValue().length()>256*1024) { // 当前文件存不下
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                buffer.putInt(0,nKeys*8+3*4+curValueSize);
                buffer.putInt(8,nKeys);

                idx = 12+nKeys*8;
                buffer.order(ByteOrder.BIG_ENDIAN);
                int local = 0;
                for (int j = i-nKeys; j < i; j++,local++) {
                    buffer.putInt(12+local*8+4,idx);
                    buffer.position(idx);
                    buffer.put(cleanKVPairs.get(j).getValue().getBytes());
                    idx += cleanKVPairs.get(j).getValue().getBytes().length;
                }

                //buff存放完毕，需存入新文件
                FileOutputStream fs = new FileOutputStream(path+"/output-"+(++fileNum)+".sst");
                FileChannel fc = fs.getChannel();
                buffer.flip();
                fc.write(buffer);
                fc.close();
                fs.close();

                //新buffer
                buffer.clear();
                buffer.order(ByteOrder.LITTLE_ENDIAN);
                idx=4;
                buffer.putInt(idx,time);//存放时间
                idx+=8;
                nKeys=0;curValueSize=0;
            }

            buffer.order(ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(idx,cleanKVPairs.get(i).getKey());
            idx +=8;
            nKeys++;
            curValueSize+=cleanKVPairs.get(i).getValue().length();
        }

        buffer.putInt(0,nKeys*8+3*4+curValueSize);
        buffer.putInt(8,nKeys);

        idx = 12+nKeys*8;
        buffer.order(ByteOrder.BIG_ENDIAN);
        int l = 0;
        for (int j = cleanKVPairs.size()-nKeys; j < cleanKVPairs.size(); j++,l++) {
            buffer.putInt(12+l*8+4,idx);
            buffer.position(idx);
            buffer.put(cleanKVPairs.get(j).getValue().getBytes());
            idx += cleanKVPairs.get(j).getValue().getBytes().length;
        }

        //buff存放完毕，需存入新文件
        FileOutputStream fs = new FileOutputStream(path+"/output-"+(++fileNum)+".sst");
        FileChannel fc = fs.getChannel();
        buffer.flip();
        fc.write(buffer);
        fc.close();
        fs.close();

        System.out.println(fileNum);
    }
}