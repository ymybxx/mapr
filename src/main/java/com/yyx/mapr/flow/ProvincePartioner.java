package com.yyx.mapr.flow;

import com.yyx.mapr.flow.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class ProvincePartioner extends Partitioner<Text,FlowBean> {


    public static HashMap<String, Integer> provinceDic = new HashMap<String, Integer>();

    static {
        provinceDic.put("138", 0);
        provinceDic.put("139", 1);
        provinceDic.put("136", 4);
        provinceDic.put("137", 2);


    }
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String prefix = text.toString().substring(0, 3);
        Integer provinceId = provinceDic.get(prefix);

        return provinceId == null ? 4 : provinceId;
    }
}
