package reducer;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import mapper.MyMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

//import db.DBOutputWritable;

public class MyReducer extends Reducer<Text, DoubleWritable, Text, Text> {

  private static final String DISCRETE_TYPE = "DISCRETE";
  private static final String NUMERIC_TYPE = "NUMERIC";
  private static final String CLASS_TYPE = "CLASS";

  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

    double sum = 0;
    int count = 0;

//    String valssz = "";
//    for (DoubleWritable val : values) {
//      valssz+= val+",";
//    }
    List<Double> caches = new ArrayList<Double>();
//    Text text = new Text(key+" => "+valssz);
//    context.write(text, new DoubleWritable(sum));

    for (DoubleWritable val : values) {
      caches.add(val.get());
      sum += val.get();
      count++;
    }

    if (key.toString().contains(MyMapper._DISCRETE)) {
      String splitter = key.toString().split("\\|")[2];
      String resWrite = splitter + "," + sum + "|" + DISCRETE_TYPE;
      Text result = new Text(resWrite + "");
      context.write(result, new Text());
    } else if (key.toString().contains(MyMapper._CLASS)) {
      String splitter = key.toString().split("\\|")[2];
      String resWrite = splitter + "," + sum + "|" + CLASS_TYPE;
      Text txt = new Text(resWrite + "");
      context.write(txt, new Text());
    } else if (key.toString().contains(MyMapper._CONTINUOUS)) {
      DoubleWritable mean = new DoubleWritable(sum / count * 1.0);
      Double calcTemp = 0.0;
      for (Double c : caches) {

        calcTemp += Math.pow(c - (sum / count * 1.0), 2);

//        context.write(new Text(key + ";"),
//            new Text(c + "|" + mean + "|" + count + " => " + calcTemp));
      }
      calcTemp = calcTemp * (1.0 / count * 1.0);
      Double result = Math.pow(calcTemp, 0.5);
      String resWrite = key.toString().split("\\|")[2];
      context.write(
          new Text(resWrite),
          new Text(";" + mean + "|" + result + "|" + NUMERIC_TYPE));
    }
  }

}