package master2018.flink;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;


import java.util.Iterator;

import static java.lang.Math.abs;

public class avgspeedfines {
    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inFilePath = args[0];
        String outFilePath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapString = source.map(new MapFunction<String, Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> map(String s) throws Exception {

                String[] fieldArray = s.split(",");
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> out = new
                        Tuple8<>(Integer.parseInt(fieldArray[0]), Integer.parseInt(fieldArray[1]), Integer.parseInt(fieldArray[2]), Integer.parseInt(fieldArray[3]),
                        Integer.parseInt(fieldArray[4]), Integer.parseInt(fieldArray[5]), Integer.parseInt(fieldArray[6]), Integer.parseInt(fieldArray[7]));
                return out;
            }
        }).filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> outFilter) throws Exception {
                if (outFilter.f6 >= 52 && outFilter.f6 <= 56) {
                    System.out.println("filter");
                    return true;
                } else {
                    return false;
                }
            }
        });

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> keyedStream = mapString.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> element) {
                return element.f0;
            }
        }).keyBy(new KeySelector<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
            @Override
            public Tuple3<Integer, Integer, Integer> getKey(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value) throws Exception {
                Tuple3<Integer, Integer, Integer> key = new Tuple3<>(value.f1, value.f3, value.f5);
                System.out.println("Keyby");
                return key;
            }
        }).window(EventTimeSessionWindows.withGap(Time.seconds(60))).apply(new WindowFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>, TimeWindow>() {
            @Override
            public void apply(Tuple3<Integer, Integer, Integer> key, TimeWindow timeWindow, Iterable<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> input, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> output) throws Exception {
                Iterator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterator = input.iterator();
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> min = iterator.next();
                Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> max = iterator.next();
                double  AvgSpd = 0;
                double convFact = 3600.0/1609.344;
                System.out.println("apply");

                for (Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> next : input) {
                    System.out.println("for");
                    if (min.f0 > next.f0) {
                        System.out.println("for primo if min");
                        min = next;
                    }
                    if (max.f0 < next.f0) {
                        System.out.println(max.f6);
                        max = next;
                    }
                }

                if (min.f6 == 52 && max.f6 == 56) {
                    System.out.println("inside if");
                    if (min.f5 == 0) {
                        System.out.println(AvgSpd);
                        AvgSpd = (abs(min.f7 - max.f7) / abs(min.f0 - max.f0)) * convFact;
                        if (AvgSpd > 60) {
                            System.out.println(AvgSpd);
                            output.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(min.f0, max.f0,min.f1,min.f3,min.f5, (int) AvgSpd));
                        }
                    } else {

                        AvgSpd = (abs(min.f7 - max.f7) / abs(min.f0 - max.f0)) * convFact;
                        System.out.println(AvgSpd);
                        if (AvgSpd > 60) {
                            output.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(max.f0, min.f0, max.f1, max.f3, max.f5, (int) AvgSpd));
                        }
                    }
                }

            }
        }).setParallelism(10);

        keyedStream.writeAsCsv(outFilePath + "/avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("VeichleTelematics");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
