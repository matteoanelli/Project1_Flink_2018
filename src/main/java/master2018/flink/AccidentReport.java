package master2018.flink;

import akka.util.ByteIterator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import java.util.Iterator;



public class AccidentReport {

    public static void main(final String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inFilePath = args[0];
        String outFilePath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(1);


        //  0    1    2     3     4    5    6    7
        //Time, VID, Spd, XWay, Lane, Dir, Seg, Pos
        //  0    1    2     3    4    5
        //Time1,VID, XWay, Seg, Dir, Pos

        //Time1, Time2, VID, XWay, Seg, Dir, Pos

        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentOut = source.map(new MapFunction<String, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String s) throws Exception {

                String[] fieldArray = s.split(",");
                Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new Tuple7<>(Integer.parseInt(fieldArray[0]),Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[6]),
                        Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[7]),Integer.parseInt(fieldArray[2]));
                return out;
            }
        }).filter(new FilterFunction<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> outFilter) throws Exception {
                if (outFilter.f6 < 5){
                    return true;
                }else{return false;}
            }
        }).setParallelism(1).keyBy(1,2,3,4,5).countWindow(4,1).apply(new WindowFunction<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple key, GlobalWindow window, Iterable<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> in, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

                Iterator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = in.iterator();
                Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer> event1 = iterator.next();

                int count = 1, tFinal = 0;

                while (iterator.hasNext()) {
                    count++;
                    if(count==4)
                        tFinal=iterator.next().f0;
                    else
                        iterator.next();
                }
                if (count == 4)
                    out.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>(event1.f0, tFinal, event1.f1, event1.f2, event1.f3, event1.f4, event1.f5));
            }
        });

        accidentOut.writeAsCsv(outFilePath+"/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try{
            env.execute("AccidentReport");
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}