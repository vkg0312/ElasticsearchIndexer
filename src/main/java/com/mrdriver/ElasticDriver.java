package com.mrdriver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.elasticsearch.hadoop.mr.EsOutputFormat;

import com.mapper.ElasticMapper;

public class ElasticDriver implements Tool{
	private Configuration conf;
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
	this.conf=conf;	
	}

	public Configuration getConf() {
		// TODO Auto-generated method stub
		
		return this.conf;
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	    conf.setBoolean("mapreduce.map.speculative", false);
	    conf.setBoolean("mapreduce.reduce.speculative", false);
	    conf.set("es.nodes", "localhost:9200");
	    //conf.set("es.net.http.auth.user", "<your_username>");
	    //conf.set("es.net.http.auth.pass", "<your_password>");
	    //conf.set("es.nodes.wan.only", "true");
	    conf.set("es.resource", "company/employees");
	    conf.set("es.input.json", "yes");

	    Job job = Job.getInstance(conf);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(EsOutputFormat.class);
	    job.setMapOutputKeyClass(NullWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    job.setJarByClass(ElasticDriver.class);
	    job.setMapperClass(ElasticMapper.class);

	    FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));

	    return job.waitForCompletion(true) ? 0 : 1;
	}
	public static void main(String[] args) throws Exception {
	    int ret = ToolRunner.run(new ElasticDriver(), args);
	    System.exit(ret);
	  }

}
