package com.hadoop.yh.event;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class JobEvent extends AbstractEvent<JobEventType> {
    private String jobID;

    public JobEvent(String jobID, JobEventType taskEventType) {
        super(taskEventType);
        this.jobID = jobID;
    }

    public String getJobID() {
        return jobID;
    }
}
