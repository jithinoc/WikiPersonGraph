package com.qburst.wikiPersonGraph.mapreduce;

import com.qburst.wikiPersonGraph.utils.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class WPGReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
    public void reduce(Text person, Iterable<Text> dependents, Context context) throws IOException, InterruptedException {
        Put put;
        Configuration configuration = context.getConfiguration();
        put = new Put(person.toString().getBytes());
        String name;

        String dependencyChain = "";
        for(Text dependent: dependents) {
            name = dependent.toString();
            if(!dependencyChain.contains(name))
                dependencyChain += name + "; ";
        }

        if(dependencyChain.contains(Constants.Person.PERSON_TAG)) {
            /* remove person_tag and owner person_name from dependencyChain */
            dependencyChain = dependencyChain.replace("**person**;", "");
            dependencyChain = dependencyChain.replace(person.toString() + "; ", "");

            byte[] columnFamily = Bytes.toBytes(configuration.get("columnFamily"));
            byte[] columnName = Bytes.toBytes(configuration.get("columnName"));
            byte[] columnValue = Bytes.toBytes(dependencyChain);
            put.add(columnFamily, columnName, columnValue);
            context.write(null, put);
        }
    }
}
