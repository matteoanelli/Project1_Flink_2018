package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class SpeedRadar {
    public SpeedRadar(String outFilePath, SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapString) {

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> out =mapString.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer,Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> outFilter) throws Exception {
                if (outFilter.f2 > 90){
                    return true;
                }else{return false;}
            }
        }).map(new MapFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>() {
            @Override
            public Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> map(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> in) throws Exception {
                return new Tuple6<>(in.f0, in.f1, in.f3,in.f6,in.f5,in.f2);
            }
        });

                //  0    1    2     3     4    5    6    7
                //Time, VID, Spd, XWay, Lane, Dir, Seg, Pos

                //Time,VID,XWay,Seg,Dir,Spd

        out.writeAsCsv(outFilePath + "/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1).name("SpeedRadar");
    }

}

