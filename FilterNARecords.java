import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterNARecords 
{
	public static class FilterNARecordsMapper
	    extends Mapper<Object, Text, Text, Text>{

	      private Text word = new Text();

	 	  public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	 		  System.out.println("Mapper: value=" + value.toString());
	 		  String record = value.toString();
	 		  if (record.length() > 0) {	      
	 			     StringTokenizer st = new StringTokenizer(record, "|");
	 			     String field1= st.nextToken();
	 			     String field2= st.nextToken();
	 			     
	 			    if ("NA".equals(field1) || "NA".equals(field2)) {
	 			    	// Skip the Record as it is invalid
	 			    	
	 			    } else {			     
	                  word.set(record);
	                  context.write(word, null);
	 			    }
	              
	          }

	      }

	  }

	   
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "Filter NA records");
	    job.setJarByClass(FilterNARecords.class);

	    job.setMapperClass(FilterNARecordsMapper.class);	
	  
	    // Sets Reducer Tasks to 0
	    job.setNumReduceTasks(0);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

