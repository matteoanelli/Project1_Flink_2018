package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public class SpeedRadar {
    public SpeedRadar(String outFilePath, SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> mapString) {

        SingleOutputStreamOperator<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out =mapString.filter(new FilterFunction<Tuple8<Integer, Integer, Integer, Integer, Integer, Integer,Integer, Integer>>() {
            @Override
            public boolean filter(Tuple8<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> outFilter) throws Exception {
                if (outFilter.f2 > 90){
                    return true;
                }else{return false;}
            }
        });

        out.writeAsCsv(outFilePath+"/speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
    }

}

