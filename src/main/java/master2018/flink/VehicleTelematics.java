package master2018.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class VehicleTelematics {
    public static void main(String[] args) {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inFilePath = args[0];
        String outFilePath = args[1];

        DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(1);
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
        }).setParallelism(1);

        SpeedRadar speedRadar = new SpeedRadar(outFilePath, mapString);
        AvgSpeedFines avgspeedfines = new AvgSpeedFines(outFilePath,mapString);
        AccidentReport accidentReport = new AccidentReport(outFilePath, mapString);


        try{
            env.execute("VeichleTelematics");
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
