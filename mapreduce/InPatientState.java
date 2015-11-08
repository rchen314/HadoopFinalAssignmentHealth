/*******************************************************************************
   For succesfully running this project we need the hadoop common and the mapred jars which should be in the lib directory of the hadoop install
 ******************************************************************************/

package com.deb.mapreduce;

import java.io.IOException;
import java.util.Iterator;


/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project the file name is hadoop-core-***.jar.
 */
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import au.com.bytecode.opencsv.CSVParser;

/**
 * @author rchen
 * @version 1.0
 * @since 28-Sep-2015
 * @package com.deb.mapreduce This program takes a CSV file of format:
 * 
 *          procedure, provider, id, city, address, state, zip, region, discharges,
 *          covered_charge, total_charge, medicare_charge
 * 
 *          and lists the average total charge for each state.
 * 
 */

public class InPatientState {

	// Mapper

	/**
	 * @author rchen
	 * @interface Mapper Map class is static and extends MapReduceBase and
	 *            implements Mapper interface having four Hadoop generics type
	 *            LongWritable, Text, Text, Text.
	 */

	public static class InPatientStateMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, FloatWritable> {

		// Define internal variable for the key
		Text k = new Text();

		/**
		 * @method map This method takes the input as a text data type and
		 *         tokenizes the input by taking "," as the delimiter. 
		 *         It outputs for each entry the procedure and the total_cost.
		 *        
		 * @method_arguments key, value, output, reporter
		 * @return void
		 *
		 *         Input format: procedure, provider, id, city, address, state, 
		 *         zip, region, discharges, covered_charge, total_charge, medicare_charge
		 */

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */

		public void map(LongWritable key, Text value,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {

			CSVParser parser = new CSVParser();
			String[] parts = parser.parseLine(value.toString());
			
			String state = new String(parts[5]);
			if (! state.equals("Provider State"))          // Don't process header 
			{
			   k.set(state);
			
			   String tc = new String(parts[10]);
			   tc = tc.substring(1);     //  Remove "$"
			   tc = tc.replace(",",  "");   // Remove internal commas
			   Float total_charge = Float.parseFloat(tc);
			   output.collect(k, new FloatWritable(total_charge));
			}
			
		} /* map */

	} /* InPatientStateMap */

	// Reducer

	/**
	 * @author rchen
	 * @interface Reducer Reduce class is static and extends MapReduceBase and
	 *            implements Reducer interface having four hadoop generics type
	 *            Text, Text, Text, Text.
	 *
	 * @method reduce This method takes key value pairs of procedure and total
	 *                cost and finds the average total cost for each procedure.
	 *                It then outputs the procedure with the highest average total cost.
	 *                
	 * @method_arguments key, values, output, reporter
	 * @return void
	 */

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
	 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
	 * org.apache.hadoop.mapred.Reporter)
	 */

	public static class InPatientStateReduce extends MapReduceBase implements
			Reducer<Text, FloatWritable, Text, FloatWritable> {


		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */

		@Override
		public void reduce(Text key, Iterator<FloatWritable> values,
				OutputCollector<Text, FloatWritable> output, Reporter reporter)
				throws IOException {

			// Define a local variable totalPrice
			float total_charge = 0;
			float avg_charge   = 0;
			long num_entries   = 0;
			
			/*
			 * Iterate through and calculate the total and get a count
			 */
			 
			while (values.hasNext()) {
				// Get the price
				float charge = values.next().get();

				total_charge += charge;
				num_entries++;
				
			}

			avg_charge = total_charge / num_entries;
			
			// Write the symbol and total volume
			output.collect(key, new FloatWritable(avg_charge));
			System.out.println(key + "    Average Charge: " + avg_charge);
			
		} /* reduce */

	} /* InPatientStateReduce */

	// Driver

	/**
	 * @method main This method is used for setting all the configuration
	 *         properties. It acts as a driver for the map reduce code.
	 * @return void
	 * @method_arguments args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {

		// Creating a JobConf object and assigning a job name for identification
		// purposes
		JobConf conf = new JobConf(InPatientState.class);
		conf.setJobName("InPatientState");

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(FloatWritable.class);

		// Setting configuration object with the Data Type of output Key and
		// Value of Mapper
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(FloatWritable.class);

		// Providing the mapper and reducer class names
		conf.setMapperClass(InPatientStateMap.class);
		conf.setReducerClass(InPatientStateReduce.class);

		// Setting format of input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// The HDFS input and output directory to be fetched from the command line
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(conf, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		
		// Running the job
		JobClient.runJob(conf);
		
	} /* main */

} /* InPatientState */
