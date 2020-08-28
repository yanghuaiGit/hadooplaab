package com.hadoop.yh.event;

import org.apache.hadoop.yarn.event.AbstractEvent;

public class TaskEvent extends AbstractEvent<TaskEventType> {
    private String taskID;

    public TaskEvent(String taskID, TaskEventType taskEventType) {
        super(taskEventType);
        this.taskID = taskID;
    }

    public String getTaskID() {
        return taskID;
    }
}
