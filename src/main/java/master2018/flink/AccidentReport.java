package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import java.util.Iterator;



public class AccidentReport {

    public AccidentReport(String outFilePath, SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapString) {

        //  0    1    2     3     4    5    6    7
        //Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

        //Time1, Time2, VID, XWay, Seg, Dir, Pos

        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> accidentOut = mapString.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> outFilter) throws Exception {
                if (outFilter.f2 == 0)
                    return true;
                else
                    return false;
            }
        }).setParallelism(1).keyBy(1,3,4,5,6,7).countWindow(4,1).apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple key, GlobalWindow window, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> in, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out) throws Exception {

                Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = in.iterator();
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> event1 = iterator.next();

                int count = 1, tFinal = 0;

                while (iterator.hasNext()) {
                    count++;
                    if(count==4)
                        tFinal=iterator.next().f0;
                    else
                        iterator.next();
                }
                if (count == 4)
                    out.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer, Integer>(event1.f0, tFinal, event1.f1, event1.f3, event1.f6, event1.f5, event1.f7));
            }
        });

        accidentOut.writeAsCsv(outFilePath+"/accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }
}