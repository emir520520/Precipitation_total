import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PrecipitationReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> precipitation, Context context)
            throws IOException, InterruptedException {
        double totalPrecipitation = 0;
        for (DoubleWritable pre: precipitation) {
            double p=pre.get();
            totalPrecipitation = totalPrecipitation+p;
        }
        context.write(key, new DoubleWritable(totalPrecipitation));
    }
}