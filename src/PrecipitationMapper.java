import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PrecipitationMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String station_year;
        double precipitation;
        try {
            String[] yearValues= values[3].split("-");
            int year= Integer.parseInt(yearValues[0]);

            station_year =  values[0]+year;
            precipitation = Double.parseDouble(values[7]);
        }
        catch (Exception e){
            station_year = "NA";
            precipitation = 0;
        }
        context.write(new Text(station_year), new DoubleWritable(precipitation));
    }
}