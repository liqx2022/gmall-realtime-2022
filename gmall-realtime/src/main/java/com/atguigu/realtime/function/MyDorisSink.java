package com.atguigu.realtime.function;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.atguigu.realtime.bean.TransientSink;
import com.atguigu.realtime.common.GmallConfig;
import com.atguigu.realtime.util.DruidDSUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author liqingxin
 * 2022/11/19 11:07
 */
public class MyDorisSink<T> extends RichSinkFunction<T>{

    private DruidDataSource druidDataSource;
    private String tableName;
    private String partitionName;

    public MyDorisSink(String tableName) {
        this(tableName, "curDate");
    }

    public MyDorisSink(String tableName, String partitionName) {
        this.tableName = tableName;
        this.partitionName = partitionName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 创建连接池
        druidDataSource = DruidDSUtil.createDorisDataSource();
    }

    @Override
    public void invoke(T bean, Context context) {
        Class<?> clazz = bean.getClass();
        Field partitionField = null;
        try {
            partitionField = clazz.getDeclaredField(partitionName);
        } catch (NoSuchFieldException e) {
            System.out.println(tableName + " 表分区属性对象获取异常~");
            e.printStackTrace();
        }
        partitionField.setAccessible(true);
        String partitionName = null;
        try {
            partitionName = (String) partitionField.get(bean);
        } catch (IllegalAccessException e) {
            System.out.println(tableName + " 表分区属性值获取异常~");
            e.printStackTrace();
        }

        Field[] declaredFields = clazz.getDeclaredFields();

        // 统计写出到 Doris 的属性个数
        int valueNum = 0;
        for (Field declaredField : declaredFields) {
            declaredField.setAccessible(true);
            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
            if (transientSink == null) {
                valueNum++;
            }
        }

        // 拼接 SQL 中的属性占位符
        StringBuilder marks = new StringBuilder();
        for (int i = 0; i < valueNum; i++) {
            marks.append("?");
            if (i < valueNum - 1) {
                marks.append(",");
            }
        }
        String commas = marks.toString();

        // 拼接用于预编译的 DML 语句
        String sql = "INSERT INTO " + GmallConfig.DORIS_SCHEMA
                + "." + tableName + "\n" +
                "partition(par" + partitionName + ") " +
                "values(" + commas + ");";
        System.out.println("向 Doris 写入数据的 SQL " + sql);

        // 获取连接对象
        DruidPooledConnection conn = null;
        try {
            conn = druidDataSource.getConnection();
        } catch (SQLException sqlException) {
            System.out.println("德鲁伊连接对象获取异常~");
            sqlException.printStackTrace();
        }

        // 获取数据库操作对象（预编译）
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = conn.prepareStatement(sql);
        } catch (SQLException sqlException) {
            System.out.println("数据库操作对象获取异常~");
            sqlException.printStackTrace();
        }

        // 获取实体类对象的属性值，依次传给占位符
        // 不会被写出的属性数量
        int skipNum = 0;
        for (int i = 0; i < declaredFields.length; i++) {
            Field declaredField = declaredFields[i];
            declaredField.setAccessible(true);
            TransientSink transientSink = declaredField.getAnnotation(TransientSink.class);
            if (transientSink == null) {
                Object value = null;
                try {
                    value = declaredField.get(bean);
                } catch (IllegalAccessException e) {
                    System.out.println("数据库操作对象传参部分实体类属性值获取异常~");
                    e.printStackTrace();
                }
                try {
                    preparedStatement.setObject(i + 1 - skipNum, value);
                } catch (SQLException sqlException) {
                    System.out.println("数据库操作对象占位符传参异常~");
                    sqlException.printStackTrace();
                }
            } else {
                skipNum++;
            }
        }

        try {
            preparedStatement.execute();
        } catch (SQLException sqlException) {
            System.out.println("Doris 写入 SQL 执行异常~");
            sqlException.printStackTrace();
        }

        closeResource(preparedStatement, conn);
    }

    /**
     * 资源释放方法
     * @param preparedStatement 数据库操作对象
     * @param conn 数据库连接对象
     */
    private void closeResource(PreparedStatement preparedStatement, DruidPooledConnection conn) {
        if(preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException sqlException) {
                System.out.println("数据库操作对象释放异常~");
                sqlException.printStackTrace();
            }
        }

        if(conn != null) {
            try {
                conn.close();
            } catch (SQLException sqlException) {
                System.out.println("德鲁伊连接对象释放异常~");
                sqlException.printStackTrace();
            }
        }
    }

}
