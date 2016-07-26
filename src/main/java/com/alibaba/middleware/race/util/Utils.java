package com.alibaba.middleware.race.util;

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

    public static void spilitCaseFile(String caseFilePath, String storeFilePath,int splitNum) throws IOException {
        int averageLine = 10906940/splitNum + 1;
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
