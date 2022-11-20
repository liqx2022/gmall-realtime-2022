package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author liqingxin
 * 2022/11/20 15:19
 */
@Data
@AllArgsConstructor

public class CartAddUuBean {
    // 窗口起始时间
    String stt;

    // 窗口闭合时间
    String edt;

    // 当天日期
    String curDate;

    // 加购独立用户数
    Long cartAddUuCt;

}
