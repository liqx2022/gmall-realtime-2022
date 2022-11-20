package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author liqingxin
 * 2022/11/20 9:10
 */

@Data
@AllArgsConstructor
public class UserLoginBean {
    // 窗口起始时间
    String stt;

    // 窗口终止时间
    String edt;

    // 当天日期
    String curDate;

    // 回流用户数
    Long backCt;

    // 独立用户数
    Long uuCt;

}
