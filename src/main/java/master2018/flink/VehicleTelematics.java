package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {
    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inFilePath = args[0];
        String outFilePath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> filterOut = source.map(new MapFunction<String, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(String s) throws Exception {

                String[] fieldArray = s.split(",");
                Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> out = new
                        Tuple6<>(Integer.parseInt(fieldArray[0]),Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[6]),
                        Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[2]));
                return out;
            }
        }).filter(new FilterFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> outFilter) throws Exception {
                if (outFilter.f5 > 90){
                    return true;
                }else{return false;}
            }
        });
        filterOut.writeAsCsv(outFilePath+"/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try{
            env.execute("VeichleTelematics");
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
