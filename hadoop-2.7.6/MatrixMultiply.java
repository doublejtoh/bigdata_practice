import java.io.IOException;
import java.util.*;
import java.util.HashMap;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiply {
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m"));
            int p = Integer.parseInt(conf.get("p"));
            String line = value.toString();
            int first_idx = line.indexOf("[");
            int end_idx = line.indexOf("]");
            line = line.substring(first_idx+1, end_idx);
            line = line.replaceAll(" ", "");
            String[] indicesAndValue = line.split(",");
            Text outputKey = new Text();
            Text outputValue = new Text();
            Log log = LogFactory.getLog(Map.class);
            log.info(indicesAndValue[0]);
            indicesAndValue[0]  = indicesAndValue[0].replace("\"", "");// doubl quote remove processing.
	    if (indicesAndValue[0].equals("a")) {
	        log.info("a condition entered:");
                for (int k = 0; k < p; k++) {
                    outputKey.set(indicesAndValue[1] + "," + k); // Row_idx,k
                    outputValue.set(indicesAndValue[0]+","+indicesAndValue[2]+","+indicesAndValue[3]); //a,Col_idx,value
                    context.write(outputKey, outputValue);
                }
            } else {
                for (int i = 0; i < m; i++) {
                    outputKey.set(i + "," + indicesAndValue[2]); // i, Col_idx
                    outputValue.set("b,"+indicesAndValue[1]+","+indicesAndValue[3]); // b,Row_idx,value
                    context.write(outputKey, outputValue);
                }
            }
            
        }
    }
    
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            HashMap<Integer, Float> hashA = new HashMap<Integer, Float>();
            HashMap<Integer, Float> hashB = new HashMap<Integer, Float>();
            for (Text val: values) {
                value = val.toString().split(",");
                if (value[0].equals("a")) {
                    hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2])); // key: col_idx, value: matrix element value.
                } else {
                    hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2])); // key: Row_idx, value: matrix element value.
                }
            }
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            float result = 0.0f;
            float m_ij;
            float n_jk;
            for (int j = 0; j < n; j++) {
                m_ij = hashA.containsKey(j) ? hashA.get(j): 0.0f;
                n_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f;
                result += m_ij * n_jk;
            }
            if (result != 0.0f) {
                Log log = LogFactory.getLog(Reduce.class);
	        log.info("entered");
	        context.write(null,
                              new Text(key.toString() + "," + Float.toString(result))); //
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.err.println("Usage: MatrixMultiply <in_dir> <out_dir>");
            System.exit(2);
        }
        
        Configuration conf = new Configuration();
        
        conf.set("m", "5");
        conf.set("n", "5");
        conf.set("p", "5");
        
        Job job = new Job(conf, "MatrixMultiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.waitForCompletion(true);
        
        
            
    }
}


