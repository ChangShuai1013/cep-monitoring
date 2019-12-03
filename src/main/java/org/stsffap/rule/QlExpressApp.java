package org.stsffap.rule;

import com.alibaba.fastjson.JSONObject;
import com.ql.util.express.DefaultContext;
import com.ql.util.express.ExpressRunner;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.stsffap.rule.entity.PropertyData;
import org.stsffap.rule.entity.QLExpressData;
import com.cloudiip.demo.UDF_JAVA;
import org.stsffap.rule.source.CalcDataSource;
import org.stsffap.rule.source.QLExpressDataSource;

import java.net.URL;
import java.net.URLClassLoader;

public class QlExpressApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //状态流
        MapStateDescriptor<String, QLExpressData> qlExpressStateDescriptor = new MapStateDescriptor<>(
                "QLExpressBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<QLExpressData>() {}));
        BroadcastStream<QLExpressData> qlDataStream = env.addSource(new QLExpressDataSource())
                .broadcast(qlExpressStateDescriptor);

        //数据流
        DataStream<Tuple2<String, JSONObject>> dataStream = env
                .addSource(new CalcDataSource())
                .map(new MapFunction<PropertyData, Tuple2<String, JSONObject>>() {
                    @Override
                    public Tuple2<String, JSONObject> map(PropertyData propertyData) throws Exception {
                        return new Tuple2<>(propertyData.getKey(), propertyData.getValue());
                    }
                });

        dataStream.connect(qlDataStream)
                .process(new BroadcastProcessFunction<Tuple2<String, JSONObject>, QLExpressData, Tuple3<String, String, String>>() {

                    @Override
                    public void processElement(Tuple2<String, JSONObject> input, ReadOnlyContext readOnlyContext, Collector<Tuple3<String, String, String>> collector) throws Exception {
                        QLExpressData qlExpressData = readOnlyContext.getBroadcastState(qlExpressStateDescriptor).get("express");
                        ExpressRunner runner = new ExpressRunner();
                        DefaultContext<String, Object> defaultContext = new DefaultContext<String, Object>();
                        defaultContext.put("a",input.f1.getIntValue("a"));
                        defaultContext.put("b",input.f1.getIntValue("b"));
                        defaultContext.put("c",input.f1.getIntValue("c"));
                        String express = qlExpressData.getExpress();
                        Object r = runner.execute(express, defaultContext, null, true, false);
                        System.out.println(express + " --- " + r);
                        collector.collect(new Tuple3<>(input.f0, r.toString(), express));
                        URL url = new URL("http://172.16.41.11:8080/demo.jar");
                        //File file = new File("http://172.16.41.11:8080/demo-1.0-SNAPSHOT.jar");
                        //URL url = file.toURI().toURL();
                        @SuppressWarnings("resource")
                        URLClassLoader myClassLoader = new URLClassLoader(new URL[]{url},Thread.currentThread().getContextClassLoader());
                        Class<?> myClass = myClassLoader.loadClass("com.cloudiip.demo.Main");
                        UDF_JAVA udf_java = (UDF_JAVA) myClass.newInstance();
                        String data = udf_java.parseData(express.getBytes(), "topic");
                        System.out.println(data);
                    }

                    @Override
                    public void processBroadcastElement(QLExpressData qlExpressData, Context context, Collector<Tuple3<String, String, String>> collector) throws Exception {
                        String express = qlExpressData.getExpress();
                        BroadcastState<String, QLExpressData> state = context.getBroadcastState(qlExpressStateDescriptor);
                        state.put("express", qlExpressData);
                    }
                })
                /*.process(new ProcessFunction<Tuple2<String, JSONObject>, Tuple2<String, String>>() {
                    @Override
                    public void processElement(Tuple2<String, JSONObject> input, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
                        ExpressRunner runner = new ExpressRunner();
                        DefaultContext<String, Object> defaultContext = new DefaultContext<String, Object>();
                        defaultContext.put("a",input.f1.getIntValue("a"));
                        defaultContext.put("b",input.f1.getIntValue("b"));
                        defaultContext.put("c",input.f1.getIntValue("c"));
                        String express = "a+b*c";
                        Object r = runner.execute(express, defaultContext, null, true, false);
                        System.out.println(express + r);
                        collector.collect(new Tuple2<>(input.f0, r.toString()));
                    }
                })*/
                .print();

        try {
            env.execute("demo");
        } catch (Exception e) {
            e.printStackTrace();
        }



    }
}
