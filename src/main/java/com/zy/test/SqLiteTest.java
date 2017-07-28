package com.zy.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SqLiteTest {

    Connection conn;

    public void testFileDb() {
        Connection c = null;
        Statement stmt = null;
        try {
            long timebegin = System.currentTimeMillis();
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:D:/sqlite/nbd/nbd.db");
            c.setAutoCommit(false);
            System.out.println("Opened database successfully");

            stmt = c.createStatement();

            ResultSet rs = stmt.executeQuery(
                    "select sum(pepole_nums) as '总人数',start_province_name as '来源地' from ol_itf_nbd_tourist_info where (start_province_name <>'') and (start_date>='2016-05-16' and start_date<='2017-05-16') and (target_province_name in('西藏','河北','新疆','台湾','四川','陕西','山西','上海','山东','青海','澳门','宁夏','内蒙古','吉林','江西','安徽','江苏','北京','湖南','重庆','福建','湖北','甘肃','香港','河南','广东','浙江','广西','云南','黑龙江','贵州','海南','辽宁')) and (start_date>='2010-05-01' and start_date<='2020-05-04') group by start_province_name order by sum(pepole_nums) desc ;");
            long timeend = System.currentTimeMillis();

            System.out.println(timeend - timebegin + "ms");

            while (rs.next()) {
                int id = rs.getInt("总人数");
                String name = rs.getString("来源地");
                System.out.println("总人数 = " + id + "," + "来源地 = " + name);
                System.out.println("-----------------");
            }
            rs.close();
            stmt.close();
            c.close();
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Operation done successfully");
    }

    public void testMemDb() {
        try {
            // 加载驱动
            Class.forName("org.sqlite.JDBC").newInstance();

            // 创建连接
            conn = DriverManager.getConnection("jdbc:sqlite::memory:");
            Statement stat = conn.createStatement();

            // 插入数据
            stat.execute("CREATE Table table_one(NAME VARCHAR)");
            for (int i = 0; i < 1000; i++)
                stat.execute("INSERT INTO table_one VALUES('this is my first program!')");

            // 查询数据
            ResultSet result = stat.executeQuery("select name from table_one ");
            int i = 1;
            while (result.next()) {
                System.out.println(i++ + ":" + result.getString("name"));
            }
            result.close();
            stat.close();
            // conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testMemDb_aa() {
        try {
            // 加载驱动
            Class.forName("org.sqlite.JDBC").newInstance();

            // 使用同样的连接，可以访问内存数据库数据
            Statement stat = conn.createStatement();

            // 查询数据
            ResultSet result = stat.executeQuery("select name from table_one ");
            int i = 1;
            while (result.next()) {
                System.out.println(i++ + ":" + result.getString("name"));
            }
            result.close();
            stat.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {

        SqLiteTest test = new SqLiteTest();
        test.testMemDb();
        test.testMemDb_aa();
    }
}
