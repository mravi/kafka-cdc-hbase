package io.svectors.hbase.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.TimerTask;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.svectors.hbase.HBaseClient;

public class HbaseTransactionTimer extends TimerTask {
    final static Logger logger = LoggerFactory.getLogger(HbaseTransactionTimer.class);
    private HBaseClient hBaseClient;
   
    public HbaseTransactionTimer(HBaseClient hBaseClient) {
        this.hBaseClient = hBaseClient;
    }

    @Override
    public void run() {
       // long lastWrittenAt = hBaseClient.getRecentTransactionTime();
        long lastWrittenAt = hBaseClient.getRecentTransactionTime();
        String id = hBaseClient.getLastWrittenUUid();
        SimpleDateFormat sdf = new SimpleDateFormat();
        sdf.setTimeZone(TimeZone.getTimeZone("EST"));
        if (StringUtils.isEmpty(id)) {
            logger.info("Hbase client Initialized at "+sdf.format(new Date(lastWrittenAt))+" (EST)");
        } else {
            logger.info("Last written "+ id +" to Hbase at "+sdf.format(new Date(lastWrittenAt))+" (EST)");
        }
        
    }

}
