package com.hadoop.yh.event;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.ApplicationClientProtocol;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;

import java.util.stream.Stream;

public class SimpleMRAppMaster extends CompositeService {
    //中央异步调度器
    private Dispatcher dispatcher;
    private String jobID;
    //该作业包含的任务数目
    private int taskNumber;
    //该作业内部包含的所有任务
    private String[] taskIDs;


    public SimpleMRAppMaster(String name, String jobID, int taskNumber) {
        super(name);
        this.jobID = jobID;
        this.taskNumber = taskNumber;
        taskIDs = new String[taskNumber];
        for (int i = 0; i < taskNumber; i++) {
            taskIDs[i] = new String(jobID + "_task_" + i);
        }
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        //定义一个中央异步调度器
        dispatcher = new AsyncDispatcher();
        //分别注册job 和 task事件调度器
        dispatcher.register(JobEventType.class, new JobEventDispatcher());
        dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
        addService((Service) dispatcher);
        super.serviceInit(conf);
    }

    public Dispatcher getDispatcher() {
        return this.dispatcher;
    }

    private class JobEventDispatcher implements EventHandler<JobEvent> {
        public void handle(JobEvent event) {
            if (event.getType() == JobEventType.JOB_KILL) {
                System.out.println("Receive JOB_KILL event,killing all the tasks ");
                Stream.of(taskIDs).forEach(item -> {
                    dispatcher.getEventHandler().handle(new TaskEvent(item, TaskEventType.T_KILL));
                });
            } else if (event.getType() == JobEventType.JOB_INIT) {
                System.out.println("Receive JOB_INIT event,scheduling the tasks ");
                Stream.of(taskIDs).forEach(item -> {
                    dispatcher.getEventHandler().handle(new TaskEvent(item, TaskEventType.T_SCHEDULE));
                });
            }
        }
    }

    private class TaskEventDispatcher implements EventHandler<TaskEvent> {
        public void handle(TaskEvent event) {
            if (event.getType() == TaskEventType.T_KILL) {
                System.out.println("Receive T_KILL event of task " + event.getTaskID());
            } else if (event.getType() == TaskEventType.T_SCHEDULE) {
                System.out.println("Receive T_SCHEDULE event of task " + event.getTaskID());
            }
        }
    }
}
