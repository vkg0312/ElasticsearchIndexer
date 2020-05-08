package com.mapper;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ElasticMapper extends Mapper<Object, Text, NullWritable, Text>{
	 private Text doc = new Text();

	    @Override
	    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	      if (value.getLength() > 0) {
	        doc.set(value);
	        context.write(NullWritable.get(), doc);
	      }
	    }
	 
}
