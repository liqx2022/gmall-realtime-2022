package com.atguigu.realtime.bean;

import lombok.Data;

/**
 * @author liqingxin
 * 2022/11/12 10:41
 */
@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}

