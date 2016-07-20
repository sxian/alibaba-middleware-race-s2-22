package com.alibaba.middleware.race.process;

import com.alibaba.middleware.race.RaceConfig;

import java.io.*;

/**
 * Created by sxian.wang on 2016/7/19.
 */
public class FileProcessor {

    public static void main(String[] args) {
        FileProcessor fp = new FileProcessor();
        try {
            fp.ReadOrder(RaceConfig.DATA_ROOT+"order.2.2");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void ReadOrder(String filePath) throws IOException {
        processData(filePath);
    }

    public void ReadBuyer(String filePath) {

    }

    public void ReadGoods(String filePath) {

    }

    public void ProcessCase(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(RaceConfig.DATA_ROOT+"order.2.2")));
        String record = br.readLine();
        while (record!=null) {
            if ((record.equals("}")||record.equals(""))) { // todo 执行查询
                record = br.readLine();
                continue;
            }

            String[] kv = record.split(":");
            switch (kv[0]) {
                case "CASE":

                    break;
                case "ORDERID":
                    break;
                case "SALERID":
                    break;
                case "GOODID":
                    break;
                case "KEYS":
                    break;
                case "STARTTIME":
                    break;
                case "ENDTIME":
                    break;
                case "Result":
                    break;
            }
            record = br.readLine();
        }
        br.close();
    }

    private void processData(String filePath) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
        String record = br.readLine();
//        while (record!=null) {
//            String[] kvs = record.split("\t");
//            for (String kv : kvs) {
//                String[] k_v = kv.split(":");
//                String key = k_v[0];
//                String value = k_v[1];
//            }
//            record = br.readLine();
//        }
        br.close();
    }

}
