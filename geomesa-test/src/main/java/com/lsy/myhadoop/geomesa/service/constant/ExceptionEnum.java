package com.lsy.myhadoop.geomesa.service.constant;

/**
 * @author xiang.ji
 */
public enum ExceptionEnum {

    SUCCESS(0, "成功"),

    /*************************通用错误码（1开头）*****************************/

    FAIL(100001, "处理失败"),

    PARAM_INVALID(100002, "参数错误"),

    NO_AUTHORITY(100003, "无权限"),

    RETRY(100004, "系统繁忙，稍后重试"),

    NOT_FOUND(100005, "查询为空"),

    INSERT_FAIL(100006, "添加失败"),

    UPDATE_FAIL(100007, "更新失败"),

    QPS_LIMIT(100009, "限速控制"),

    AUTH_FAILED(100010, "SSO验证失败"),

    HTTP_FAILED(100011, "HTTP请求失败"),

    SSO_FAIL(100012, "SSO验证失败"),

    TIME_OUT(100013, "请求超时"),

    IDEMPOTENT(100014, "密等校验，操作频繁，稍后重试"),

    /**************************数据管理类错误码 （2开头）*****************************/

    DATA_MANAGER_HIVE_SUG(200001, "查询失败"),

    DATA_MANAGER_TASK_STATUS_ERR(200002, "任务状态失败"),

    DATA_MANAGER_TASK_LOG_READ_ERR(200003, "任务日志读取失败"),

    SQL_EXCEPTION(200004, "sql执行失败"),

    USER_INFO_FAILED(200005, "用户信息查询失败"),

    CONFIG_TABLES_NULL(200006, "配置默认表列表为空"),

    CONFIG_TABLE_FORMAT_ERR(200007, "配置默认表列表项不是database.table格式"),

    WEEK_STORAGE_ERR(200008, "获取本周新增存储失败"),

    CREATE_DATABASE_FAILED(200009, "创建数据库失败"),

    /**************************运维中心类错误码 （3开头）*****************************/


    /**************************调度中心类错误码 （4开头）*****************************/


    /**************************发布中心类错误码 （5开头）*****************************/
    OPS_TASK_VERIFY_FLOW_CREATE_ERR(500001, "审核任务已存在"),
    OPS_AUTH_ERR(500000, "项目权限错误"),

    /**************************数据服务API错误码 （6开头）*****************************/
    PROTOCAL_ERROR(600001, "协议错误"),

    UPDATE_API_ERROR(600002, "更新api错误"),

    UPDATE_API_REPEAT_ERROR(600003, "api 名字或者api path已经存在"),

    API_STATUS_DRAFT_ERROR(600004, "非草稿状态"),

    UPDATE_API_NO_AUTH(600005, "没有更新此api的权限"),

    UPDATE_API_PARM_NO_AUTH(600006, "没有更新此api参数的权限"),

    UPDATE_API_PARM_REPEAT_ERROR(600007, "此api的参数名字重复"),

    OPERATE_API_STATUS_ERROR(600008, "操作的前置状态错误"),

    OPERATE_API_ERROR(600009, "API操作错误"),

    API_INFO_MISS(600010, "API信息缺失"),

    USER_PROJECT_INFO_FAILED(600011, "用户项目信息查询失败"),

    NO_PROJECT_API(600012, "不是用户所属项目的api"),

    API_AUTH_INFO_ERROR(600013, "用户验证信息错误"),

    NO_PROJECT_INFO(600014, "用户未加入任何项目"),

    APPLY_API_STATUS_ERROR(601000, "申请API状态错误"),

    APPLY_API_UPDATE_STATUS_ERROR(601001, "申请API更新状态错误"),

    /*************************默认错误码（9开头）*****************************/

    SYSTEM_ERROR(999999, "系统错误");

    private int code;

    private String message;

    ExceptionEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
