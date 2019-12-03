package org.stsffap.state.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stsffap.rule.entity.QLExpressData;

/**
 * @allLen  数据总长度.
 * @metaLocs 原始数据位置.
 *
 */
public class QLExpressDataSource extends RichSourceFunction<QLExpressData> {
    private  static Logger logger = LoggerFactory.getLogger(QLExpressDataSource.class);
    private boolean flag = true;
    @Override
    public void run(SourceContext<QLExpressData> ctx) throws Exception {
        logger.info("ql source run");
        while (true){
            ctx.collect(QLExpressData.genQlExpressData(flag));
            flag = !flag;
            Thread.sleep(5000);
        }
    }

    @Override
    public void cancel() {
        System.out.println("ql source canceled");
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        logger.info("ql source open");
    }
    @Override
    public void close(){
        logger.info("ql source closed");
    }

    public QLExpressDataSource(){
    }
}
