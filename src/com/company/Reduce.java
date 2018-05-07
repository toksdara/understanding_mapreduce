package com.company;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Toks on 22/12/2016.
 */
public class Reduce extends Reducer <Text, IntWritable, Text, IntWritable> {
    @Override
    public void reduce (Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException{
        int count =0;
        for (IntWritable value:values)
        {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }

}
