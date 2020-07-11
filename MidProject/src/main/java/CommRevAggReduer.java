import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.poi.ss.formula.functions.T;

import java.io.IOException;

public class CommRevAggReduer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException {
        double daily_total_fare= 0.0, daily_avg_fare=0.0;
        for (Text val : values){
            daily_total_fare += Double.parseDouble(val.toString().split(":")[0]);
            daily_avg_fare += Double.parseDouble(val.toString().split(":")[0]);
        }
        context.write(new Text(key.toString()+","+ String.valueOf(daily_total_fare)+","+String.valueOf(daily_avg_fare) ), null);
    }
}
