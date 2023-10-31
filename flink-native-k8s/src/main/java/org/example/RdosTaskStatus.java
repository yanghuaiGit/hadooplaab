/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example;


import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public enum RdosTaskStatus implements Serializable {
    UNSUBMIT(0),
    CREATED(1),
    SCHEDULED(2),
    DEPLOYING(3),
    RUNNING(4),
    FINISHED(5),
    CANCELLING(6),
    CANCELED(7),
    FAILED(8),
    SUBMITFAILD(9),
    SUBMITTING(10),
    RESTARTING(11),
    MANUALSUCCESS(12),
    KILLED(13),
    SUBMITTED(14),
    NOTFOUND(15),
    WAITENGINE(16),
    WAITCOMPUTE(17),
    FROZEN(18),
    ENGINEACCEPTED(19),
    ENGINEDISTRIBUTE(20),
    PARENTFAILED(21),
    FAILING(22),
    COMPUTING(23),
    EXPIRE(24),
    LACKING(25),
    AUTOCANCELED(26),
    RUNNING_TASK_RULE(27);

    private int status;
    private static final List<Integer> CAN_STOP_STATUS = Lists.newArrayList(new Integer[]{UNSUBMIT.getStatus(), CREATED.getStatus(), SCHEDULED.getStatus(), DEPLOYING.getStatus(), RUNNING.getStatus(), SUBMITTING.getStatus(), RESTARTING.getStatus(), SUBMITTED.getStatus(), WAITENGINE.getStatus(), WAITCOMPUTE.getStatus(), LACKING.getStatus(), NOTFOUND.getStatus(), RUNNING_TASK_RULE.getStatus()});
    public static final List<Integer> STOPPED_STATUS = Lists.newArrayList(new Integer[]{MANUALSUCCESS.getStatus(), PARENTFAILED.getStatus(), FAILED.getStatus(), CANCELED.getStatus(), SUBMITFAILD.getStatus(), KILLED.getStatus(), FINISHED.getStatus(), EXPIRE.getStatus(), FROZEN.getStatus(), AUTOCANCELED.getStatus()});
    private static final List<Integer> CAN_RESTART_STATUS = Lists.newArrayList(new Integer[]{FINISHED.getStatus(), CANCELED.getStatus(), SUBMITFAILD.getStatus(), FAILED.getStatus(), MANUALSUCCESS.getStatus(), UNSUBMIT.getStatus(), KILLED.getStatus(), PARENTFAILED.getStatus(), EXPIRE.getStatus(), LACKING.getStatus(), AUTOCANCELED.getStatus()});
    private static final Logger logger = LoggerFactory.getLogger(RdosTaskStatus.class);
    private static final long serialVersionUID = 1L;
    public static final List<Integer> UNSUBMIT_STATUS = Lists.newArrayList(new Integer[]{UNSUBMIT.getStatus()});
    public static final List<Integer> RUNNING_STATUS = Lists.newArrayList(new Integer[]{RUNNING.getStatus(), NOTFOUND.getStatus(), RUNNING_TASK_RULE.getStatus(), FAILING.getStatus()});
    public static final List<Integer> FINISH_STATUS = Lists.newArrayList(new Integer[]{FINISHED.getStatus(), MANUALSUCCESS.getStatus()});
    public static final List<Integer> FAILED_STATUS = Lists.newArrayList(new Integer[]{FAILED.getStatus(), SUBMITFAILD.getStatus(), PARENTFAILED.getStatus()});
    public static final List<Integer> SUBMITFAILD_STATUS = Lists.newArrayList(new Integer[]{SUBMITFAILD.getStatus()});
    public static final List<Integer> PARENTFAILED_STATUS = Lists.newArrayList(new Integer[]{PARENTFAILED.getStatus()});
    public static final List<Integer> RUN_FAILED_STATUS = Lists.newArrayList(new Integer[]{FAILED.getStatus()});
    public static final List<Integer> WAIT_STATUS = Lists.newArrayList(new Integer[]{WAITENGINE.getStatus(), WAITCOMPUTE.getStatus(), RESTARTING.getStatus(), SUBMITTED.getStatus(), ENGINEACCEPTED.getStatus(), ENGINEDISTRIBUTE.getStatus(), SCHEDULED.getStatus(), CREATED.getStatus(), DEPLOYING.getStatus(), COMPUTING.getStatus(), LACKING.getStatus()});
    public static final List<Integer> SUBMITTING_STATUS = Lists.newArrayList(new Integer[]{SUBMITTING.getStatus()});
    public static final List<Integer> STOP_STATUS = Lists.newArrayList(new Integer[]{KILLED.getStatus(), CANCELED.getStatus(), EXPIRE.getStatus(), AUTOCANCELED.getStatus()});
    public static final List<Integer> EXPIRE_STATUS = Lists.newArrayList(new Integer[]{EXPIRE.getStatus(), AUTOCANCELED.getStatus()});
    public static final List<Integer> FROZEN_STATUS = Lists.newArrayList(new Integer[]{FROZEN.getStatus()});
    private static final List<Integer> UNFINISHED_STATUSES = Lists.newArrayList(new Integer[]{RUNNING.getStatus(), UNSUBMIT.getStatus(), RESTARTING.getStatus(), SUBMITTING.getStatus()});
    private static final List<Integer> UN_SUBMIT_STATUSES = Lists.newArrayList(new Integer[]{ENGINEACCEPTED.getStatus(), UNSUBMIT.getStatus()});
    private static final Map<Integer, List<Integer>> COLLECTION_STATUS;
    private static final Map<Integer, List<Integer>> STREAM_STATUS;
    private static final Map<Integer, List<Integer>> STATUS_FAILED_DETAIL;
    private static final Map<Integer, List<Integer>> STATUS_FAILED_DETAIL_EXPIRE;

    private RdosTaskStatus(int status) {
        this.status = status;
    }

    public Integer getStatus() {
        return this.status;
    }

    public static RdosTaskStatus getTaskStatus(String taskStatus) {
        if (Strings.isNullOrEmpty(taskStatus)) {
            return null;
        } else if ("ERROR".equalsIgnoreCase(taskStatus)) {
            return FAILED;
        } else if ("RESTARTING".equalsIgnoreCase(taskStatus)) {
            return RUNNING;
        } else if ("INITIALIZING".equalsIgnoreCase(taskStatus)) {
            return SCHEDULED;
        } else if ("SUSPENDED".equalsIgnoreCase(taskStatus)) {
            return FINISHED;
        } else if ("RECONCILING".equalsIgnoreCase(taskStatus)) {
            return WAITENGINE;
        } else {
            try {
                return valueOf(taskStatus);
            } catch (Exception var2) {
                logger.info("No enum constant :" + taskStatus);
                return null;
            }
        }
    }

    public static RdosTaskStatus getTaskStatus(int status) {
        RdosTaskStatus[] var1 = values();
        int var2 = var1.length;

        for(int var3 = 0; var3 < var2; ++var3) {
            RdosTaskStatus tmp = var1[var3];
            if (tmp.getStatus() == status) {
                return tmp;
            }
        }

        return null;
    }

    public static boolean needClean(Integer status) {
        return STOPPED_STATUS.contains(status) || RESTARTING.getStatus().equals(status);
    }

    public static boolean canStart(Integer status) {
        return SUBMITTING.getStatus().equals(status) || UNSUBMIT.getStatus().equals(status);
    }

    public static boolean canRestart(Integer status) {
        return CAN_RESTART_STATUS.contains(status);
    }

    public static boolean canReset(Integer currStatus) {
        return STOPPED_STATUS.contains(currStatus) || UNSUBMIT.getStatus().equals(currStatus);
    }

    public static List<Integer> getCanStopStatus() {
        return CAN_STOP_STATUS;
    }

    public static boolean isStopped(Integer status) {
        return STOPPED_STATUS.contains(status);
    }

    public static List<Integer> getStoppedStatus() {
        return STOPPED_STATUS;
    }

    public static List<Integer> getFinishStatus() {
        return FINISH_STATUS;
    }

    public static List<Integer> getWaitStatus() {
        return WAIT_STATUS;
    }

    public static String getCode(Integer status) {
        String key = null;
        if (FINISH_STATUS.contains(status)) {
            key = FINISHED.name();
        } else if (RUNNING_STATUS.contains(status)) {
            key = RUNNING.name();
        } else if (PARENTFAILED_STATUS.contains(status)) {
            key = PARENTFAILED.name();
        } else if (SUBMITFAILD_STATUS.contains(status)) {
            key = SUBMITFAILD.name();
        } else if (RUN_FAILED_STATUS.contains(status)) {
            key = FAILED.name();
        } else if (UNSUBMIT_STATUS.contains(status)) {
            key = UNSUBMIT.name();
        } else if (WAIT_STATUS.contains(status)) {
            key = WAITENGINE.name();
        } else if (SUBMITTING_STATUS.contains(status)) {
            key = SUBMITTING.name();
        } else if (STOP_STATUS.contains(status)) {
            key = CANCELED.name();
        } else if (FROZEN_STATUS.contains(status)) {
            key = FROZEN.name();
        } else {
            key = UNSUBMIT.name();
        }

        return key;
    }

    public static List<Integer> getUnfinishedStatuses() {
        return UNFINISHED_STATUSES;
    }

    public static List<Integer> getUnSubmitStatus() {
        return UN_SUBMIT_STATUSES;
    }

    public static List<Integer> getCollectionStatus(Integer status) {
        return (List)COLLECTION_STATUS.computeIfAbsent(status, (k) -> {
            return new ArrayList(0);
        });
    }

    public static Map<Integer, List<Integer>> getCollectionStatus() {
        return COLLECTION_STATUS;
    }

    public static Map<Integer, List<Integer>> getStatusFailedDetail() {
        return STATUS_FAILED_DETAIL;
    }

    public static Map<Integer, List<Integer>> getStatusStream() {
        return STREAM_STATUS;
    }

    public static Map<Integer, List<Integer>> getStatusFailedDetailAndExpire() {
        return STATUS_FAILED_DETAIL_EXPIRE;
    }

    public static int getShowStatus(Integer status) {
        if (FAILED_STATUS.contains(status)) {
            status = FAILED.getStatus();
        } else {
            status = getShowStatusWithoutStop(status);
        }

        return status;
    }

    public static int getShowStatusWithoutStop(Integer status) {
        if (FINISH_STATUS.contains(status)) {
            status = FINISHED.getStatus();
        } else if (RUNNING_STATUS.contains(status)) {
            status = RUNNING.getStatus();
        } else if (UNSUBMIT_STATUS.contains(status)) {
            status = UNSUBMIT.getStatus();
        } else if (WAIT_STATUS.contains(status)) {
            status = WAITENGINE.getStatus();
        } else if (SUBMITTING_STATUS.contains(status)) {
            status = SUBMITTING.getStatus();
        } else if (EXPIRE_STATUS.contains(status)) {
            status = EXPIRE.getStatus();
        } else if (STOP_STATUS.contains(status)) {
            status = CANCELED.getStatus();
        } else if (FROZEN_STATUS.contains(status)) {
            status = FROZEN.getStatus();
        }

        return status;
    }

    public static String getShowStatusDesc(Integer status) {
        int showStatus = getShowStatusWithoutStop(status);
        RdosTaskStatus taskStatus = getTaskStatus(showStatus);
        switch (taskStatus) {
            case FINISHED:
                return "成功";
            case RUNNING:
                return "运行中";
            case UNSUBMIT:
                return "等待提交";
            case SUBMITTING:
                return "提交中";
            case EXPIRE:
                return "自动取消";
            case CANCELED:
                return "手动取消";
            case FROZEN:
                return "冻结";
            case WAITENGINE:
                return "等待运行";
            case FAILED:
                return "运行失败";
            case SUBMITFAILD:
                return "提交失败";
            default:
                return "";
        }
    }

    public static List<Integer> getStoppedAndNotFound() {
        List<Integer> status = new ArrayList(STOPPED_STATUS);
        status.add(NOTFOUND.getStatus());
        return status;
    }

    static {
        UNFINISHED_STATUSES.addAll(WAIT_STATUS);
        COLLECTION_STATUS = new HashMap();
        COLLECTION_STATUS.put(UNSUBMIT.getStatus(), Lists.newArrayList(new Integer[]{UNSUBMIT.getStatus()}));
        COLLECTION_STATUS.put(RUNNING.getStatus(), RUNNING_STATUS);
        COLLECTION_STATUS.put(FINISHED.getStatus(), FINISH_STATUS);
        COLLECTION_STATUS.put(FAILED.getStatus(), FAILED_STATUS);
        COLLECTION_STATUS.put(WAITENGINE.getStatus(), WAIT_STATUS);
        COLLECTION_STATUS.put(SUBMITTING.getStatus(), Lists.newArrayList(new Integer[]{SUBMITTING.getStatus()}));
        COLLECTION_STATUS.put(CANCELED.getStatus(), STOP_STATUS);
        COLLECTION_STATUS.put(FROZEN.getStatus(), Lists.newArrayList(new Integer[]{FROZEN.getStatus()}));
        STREAM_STATUS = new HashMap();
        STREAM_STATUS.put(CANCELED.getStatus(), STOP_STATUS);
        STREAM_STATUS.put(RUNNING.getStatus(), RUNNING_STATUS);
        STREAM_STATUS.put(FAILED.getStatus(), FAILED_STATUS);
        STATUS_FAILED_DETAIL = new HashMap();
        STATUS_FAILED_DETAIL.put(UNSUBMIT.getStatus(), Lists.newArrayList(new Integer[]{UNSUBMIT.getStatus()}));
        STATUS_FAILED_DETAIL.put(RUNNING.getStatus(), Lists.newArrayList(new Integer[]{RUNNING.getStatus(), NOTFOUND.getStatus(), RUNNING_TASK_RULE.getStatus()}));
        STATUS_FAILED_DETAIL.put(FINISHED.getStatus(), FINISH_STATUS);
        STATUS_FAILED_DETAIL.put(FAILED.getStatus(), Lists.newArrayList(new Integer[]{FAILED.getStatus(), FAILING.getStatus()}));
        STATUS_FAILED_DETAIL.put(SUBMITFAILD.getStatus(), Lists.newArrayList(new Integer[]{SUBMITFAILD.getStatus()}));
        STATUS_FAILED_DETAIL.put(PARENTFAILED.getStatus(), Lists.newArrayList(new Integer[]{PARENTFAILED.getStatus()}));
        STATUS_FAILED_DETAIL.put(WAITENGINE.getStatus(), WAIT_STATUS);
        STATUS_FAILED_DETAIL.put(SUBMITTING.getStatus(), Lists.newArrayList(new Integer[]{SUBMITTING.getStatus()}));
        STATUS_FAILED_DETAIL.put(CANCELED.getStatus(), Lists.newArrayList(new Integer[]{KILLED.getStatus(), CANCELED.getStatus()}));
        STATUS_FAILED_DETAIL.put(EXPIRE.getStatus(), EXPIRE_STATUS);
        STATUS_FAILED_DETAIL.put(FROZEN.getStatus(), Lists.newArrayList(new Integer[]{FROZEN.getStatus()}));
        STATUS_FAILED_DETAIL_EXPIRE = new HashMap();
        STATUS_FAILED_DETAIL_EXPIRE.putAll(STATUS_FAILED_DETAIL);
    }
}
