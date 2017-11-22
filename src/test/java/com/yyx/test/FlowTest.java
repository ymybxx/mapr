package com.yyx.test;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class FlowTest {

    static  String tmpPath = "/Users/inequality/tmp";


    @Test

    public void testBean() {
        List<String> list = new ArrayList<String>();
        list.add("111");
        list.add("222");
        list.add("333");
        list.add("444");

        Iterable<String> iterable  = (Iterable<String>) list.iterator();
        List<String> re = new ArrayList<String>();
        String r  = null;
        for (String s : iterable) {
            re.add(s);
        }

        System.out.println(re);

    }
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
