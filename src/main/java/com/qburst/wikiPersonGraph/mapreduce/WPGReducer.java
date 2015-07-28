package com.qburst.wikiPersonGraph.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WPGReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        Put put;
        String rowKey = key.toString();
        Configuration configuration = context.getConfiguration();
        String persons = "";
        if(rowKey.equals(""))
            rowKey = "null";
        put = new Put(rowKey.getBytes());
        String token;

        for(Text value: values) {
            token = value.toString();
            if(!persons.contains(token))
                persons += token + "; ";
        }

        if(persons.contains("**person**")) {
            persons = persons.replace("**person**,", "");
            persons = persons.replace(rowKey + "; ", "");
            byte[] columnFamily = Bytes.toBytes(configuration.get("columnFamily"));
            byte[] columnName = Bytes.toBytes(configuration.get("columnName"));
            byte[] columnValue = Bytes.toBytes(persons);
            put.add(columnFamily, columnName, columnValue);
            context.write(null, put);
        }
    }
}
