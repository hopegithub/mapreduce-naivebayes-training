package mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

  private final static DoubleWritable one = new DoubleWritable(1);
  private Text wordAttr = new Text();

  private Text wordClass = new Text();

  public final static String _CLASS = "|_class|";
  public final static String _DISCRETE = "|disc|";
  public final static String _CONTINUOUS = "|cont|";

//  private final static String NUMERIC_TYPE = "NUMERICAL";
  private final static String DISCRETE_TYPE = "DISCRETE";

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

    String val = value.toString();
    String[] inputSplit = val.split(",");

    String classConf = context.getConfiguration().get("classes");
    String[] classSplitConf = classConf.split(";");

    String attrConf = context.getConfiguration().get("attributes");
    String[] attrSplitConf = attrConf.split(";");

    int checkerClassPrior = classSplitConf.length;
    /* New Impl */

		/* For Attr */
    for (int i = 0; i < attrSplitConf.length; i++) {
      if(attrSplitConf[i].split(",")[2].equals(DISCRETE_TYPE)){
        for (int j = 0; j < classSplitConf.length; j++) {
          String currKey = _DISCRETE + attrSplitConf[i].split(",")[0]
              + "," + inputSplit[Integer.parseInt(attrSplitConf[i]
              .split(",")[1])]
              + "," + classSplitConf[j].split(",")[0]
              + "," + inputSplit[Integer.parseInt(classSplitConf[j]
              .split(",")[1])];
          wordAttr.set(currKey);
          context.write(wordAttr, one);

				/* DEBUG ABOVE */
				/* Calculate Class Prior Probability */
          /** Untuk iterasi attrSplit ke dua
           * sudah tidak menghitung class prior lagi ! */
          if (checkerClassPrior > 0) {
            String currClassKey = _CLASS + classSplitConf[j].split(",")[0]
                + "," + inputSplit[Integer.parseInt(classSplitConf[j]
                .split(",")[1])];
            wordClass.set(currClassKey);
            context.write(wordClass, one);
            checkerClassPrior--;
          }
        }
      }else{
        for(int j = 0; j < classSplitConf.length; j++){

          String currKey = _CONTINUOUS + attrSplitConf[i].split(",")[0] +
              ","+classSplitConf[j].split(",")[0] +
              ","+inputSplit[Integer.parseInt(classSplitConf[j]
              .split(",")[1])];
          wordAttr.set(currKey);
          DoubleWritable valNum = new DoubleWritable(Double
              .parseDouble(inputSplit[Integer
                  .parseInt(attrSplitConf[i].split(",")[1])]));
          context.write(wordAttr,valNum);
        }
      }
    }
  }
}
