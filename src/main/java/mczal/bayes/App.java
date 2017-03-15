package mczal.bayes;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import db.DBOutputWritable;
import mapper.MyMapper;
import reducer.MyReducer;

// Taken from https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Source_Code
public class App {

  public static final String HDFS_AUTHORITY = "hdfs://localhost:9000";

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      String argsExcp =
          "Error catched by custom Impl. Please read the following line below.\n"
              + "-----------------------------\n"
              + "-> Arguments must only consist of 1 path.\n"
              + "-> It located the model of input you want to execute in HDFS.\n"
              + "-> Ex: \n"
              + "-> If args[0]=/user/root/bayes/weather -> \n"
              + "-> Then, that path must had : \n"
              + "-> (1) info path + file => /user/root/bayes/weather/info/meta.info\n"
              + "-> (2) input path + file => /user/root/bayes/weather/input/...\n"
              + "-> (3) testing path for input split file => /user/root/bayes/weather/testing/input/...\n"
              + "-> The output file will be located in /user/root/bayes/weather/output/...\n"
              + "-----------------------------";
      throw new IllegalArgumentException(argsExcp);
    }

    String inputPath = args[0];
    String outputPath = args[0];
    String infoPathFile = args[0];
    if (args[0].charAt(args[0].length() - 1) == '/') {
      inputPath += "input";
      outputPath += "output";
      infoPathFile += "info/meta.info";
    } else {
      inputPath += "/input";
      outputPath += "/output";
      infoPathFile += "/info/meta.info";
    }

    String cols = "predictor,pred_val,class_name,class_val,count";
    Configuration conf = new Configuration();

    FileSystem fs = FileSystem.get(conf);
    /* Check if output path (args[1])exist or not */
    if (fs.exists(new Path(outputPath))) {
      /* If exist delete the output path */
      fs.delete(new Path(outputPath), true);
    }

    Path path = new Path(HDFS_AUTHORITY + infoPathFile);
    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
    String currClass = br.readLine();
    String currAttr = br.readLine();

    conf.set("classes", currClass.split(":")[1]);
    conf.set("attributes", currAttr.split(":")[1]);

    Job job = Job.getInstance(conf, "bayes");
    job.setJarByClass(App.class);
    job.setMapperClass(MyMapper.class);
//    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);
    job.setMapOutputKeyClass(Text.class); // your mapper - not shown in this example
    job.setMapOutputValueClass(DoubleWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}