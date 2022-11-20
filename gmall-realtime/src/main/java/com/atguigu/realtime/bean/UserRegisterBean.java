package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author liqingxin
 * 2022/11/20 14:33
 */
@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口终止时间
    String edt;
    // 当天日期
    String curDate;
    // 注册用户数
    Long registerCt;

}
