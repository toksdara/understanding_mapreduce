package com.company;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Toks on 22/12/2016.
 */
public class Map extends Mapper <LongWritable, Text, Text, IntWritable> {
@Override

    public void map (LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
        //String[] row = value.toString().split("\\t");
    String[] row = value.toString().split(",");

    //for (int i = 0; i < row.length; i++) {
    //    String x = row[i];
    //    context.write(new Text(x), new IntWritable(1));

    /*
    if (row.length > 0){
        System.out.println("Arrays not empty");
        return;
    }
    */

    /*
    System.out.println(row[0]);
    System.out.println(row[1]);
    System.out.println(row[2]);
    return;
    */

    context.write(new Text(row[2]), new IntWritable(1));
    }
}
