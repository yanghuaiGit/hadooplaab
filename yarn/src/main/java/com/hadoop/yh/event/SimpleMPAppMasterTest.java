package com.hadoop.yh.event;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@SuppressWarnings("unchecked")
public class SimpleMPAppMasterTest {

    public static void main(String[] args) throws Exception {
        String jobID  = "job_2020";
        SimpleMRAppMaster appMaster = new SimpleMRAppMaster("simple MRAppMaster", jobID, 5);
        YarnConfiguration conf = new YarnConfiguration(new Configuration());
        appMaster.init(conf);
        appMaster.start();
        appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID,JobEventType.JOB_KILL));
        appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID,JobEventType.JOB_INIT));
    }
}
