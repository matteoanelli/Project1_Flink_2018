package master2018.flink;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

import static java.lang.Math.abs;

public class AvgSpeedFines {
    public AvgSpeedFines(String outFilePath, SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapString) {

        //  0    1    2     3     4    5    6    7
        //Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> out = mapString.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> outFilter) throws Exception {
                if (outFilter.f6 >= 52 && outFilter.f6 <= 56)
                    return true;
                else
                    return false;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                return element.f0 * 1000;
            }
        }).keyBy(new KeySelector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple3<Integer, Integer, Integer> getKey(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                Tuple3<Integer, Integer, Integer> key = new Tuple3<>(value.f1, value.f3, value.f5);
                return key;
            }
        }).window(EventTimeSessionWindows.withGap(Time.seconds(31))).apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Double>, Tuple3<Integer, Integer, Integer>, TimeWindow>() {
            @Override
            public void apply(Tuple3<Integer, Integer, Integer> key, TimeWindow timeWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Double>> output) throws Exception {
                Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> min, max;
                min = max = iterator.next();
                double AvgSpd = 0;
                double convFact = 2.236936292054402;

                for (Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next : input) {
                    if (min.f7 > next.f7)
                        min = next;
                    if (max.f7 < next.f7)
                        max = next;
                }
                AvgSpd = (abs(min.f7 - max.f7) * 1.0 / abs(min.f0 - max.f0)) * convFact;
                if (min.f6 == 52 && max.f6 == 56 && AvgSpd > 60) {

                    if (min.f5 == 0)
                        output.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>(min.f0, max.f0, min.f1, min.f3, min.f5, AvgSpd));
                    else
                        output.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Double>(max.f0, min.f0, max.f1, max.f3, max.f5, AvgSpd));


                }

            }
        });

        out.writeAsCsv(outFilePath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("AvgSpeedFines");

    }
}
