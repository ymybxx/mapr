package com.yyx.test;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;

public class FlowTest {

    static  String tmpPath = "/Users/inequality/tmp";

    @Test

    public void readFile() throws Exception{

        String flowPath = tmpPath + "/HTTP_20130313143750.dat";
        File flowFile = new File(flowPath);;
        BufferedReader bf = new BufferedReader(new InputStreamReader(new FileInputStream(flowFile)));
        String  line ;
//        while ((line = bf.readLine()) != null) {
//            System.out.println(line);
//        }
        int index = 0;
        line = bf.readLine();
        String[] words = line.split("\t");
        for (String word : words
             ) {
            System.out.println(word + "-----" + (++index));
        }
    }
}
