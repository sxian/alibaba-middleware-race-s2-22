package com.alibaba.middleware.race.util;

import com.alibaba.middleware.race.OrderSystemImpl;

import java.io.*;

/**
 * Created by sxian.wang on 2016/7/21.
 */
public class Utils {
    public static BufferedWriter createWriter(String file) throws IOException {
        return new BufferedWriter(new FileWriter(file));
    }

    public static BufferedReader createReader(String file) throws FileNotFoundException {
        return new BufferedReader(new FileReader(file));
    }

    public static OrderSystemImpl.Row createRow(String row) {
        return OrderSystemImpl.createRow(row);
    }

    public static void spilitCaseFile(String caseFilePath, String storeFilePath,int splitNum) throws IOException {
        int averageLine = 10906940/splitNum + 1;
        try {
            BufferedReader br = createReader(storeFilePath+"flag");
            String flag = br.readLine();
            if (flag.equals(String.valueOf(splitNum))) {
                return;
            } else {
            }

        } catch (Exception e) {

        } finally {
            BufferedWriter bw = createWriter(storeFilePath+"flag");
            bw.write(String.valueOf(splitNum));
            bw.flush();
            bw.close();
        }
        long count = 0;
        int index = 0;
        BufferedReader bw = createReader(caseFilePath);
        BufferedWriter br = createWriter(storeFilePath+index++);
        String line = bw.readLine();
        while (line!=null) {
            if (count>=averageLine) {
                if (line.startsWith(("CASE"))) {
                    br.flush();
                    br.close();
                    br = createWriter(storeFilePath+index++);
                    count = 1;
                    System.out.println("split one file");
                }
            }
            br.write(line+"\n");
            count++;
            line = bw.readLine();
        }
        br.flush();
        br.close();
        System.out.print(count);
    }

}
