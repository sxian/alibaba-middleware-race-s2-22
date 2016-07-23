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

}
