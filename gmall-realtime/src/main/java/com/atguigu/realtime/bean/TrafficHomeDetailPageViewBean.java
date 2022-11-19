package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author liqingxin
 * 2022/11/19 23:00
 */
@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 当天日期
    String curDate;

    // 首页独立访客数
    Long homeUvCt;

    // 商品详情页独立访客数
    Long goodDetailUvCt;
}
