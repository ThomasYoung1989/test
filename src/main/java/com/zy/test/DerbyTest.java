package com.zy.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.h2.tools.Server;

import com.zy.util.FileUtils;

public class DerbyTest {
    private Server server;

    private String port = "8082";

    private static String sourceURL1 = "jdbc:derby://localhost:1527/memory:myDB";

    public void testH2() {
        try {
            // 加载驱动
            Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();

            // 创建连接
            Connection conn = DriverManager.getConnection(sourceURL1 + ";create=true");
            Statement stat = conn.createStatement();

            // 插入数据
            // stat.execute("CREATE Table table_one (NAME varchar(30))");
            for (int i = 0; i < 10000; i++)
                stat.execute("INSERT INTO table_one VALUES('this is my first program!')");

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

    public void test_insert_nbd() {
        try {
            // 加载驱动
            Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();

            // 创建连接
            Connection conn = DriverManager.getConnection(sourceURL1 + ";create=true");
            Statement stat = conn.createStatement();

            // stat.execute("drop table ol_itf_nbd_tourist_info");
            // // stat.execute("create table ol_itf_nbd_tourist_info (id int NOT NULL)");
            // // 插入数据

            List<String> sqlArray = FileUtils.readFileToStringArray("C:/Users/zhangyang11/Desktop/test_data/temp1.txt");

            for (int i = 1; i <= sqlArray.size(); i++) {
                String sql = sqlArray.get(i - 1);
                stat.addBatch(sql);
                if (i % 1000 == 0) {
                    stat.executeBatch();
                    stat.clearBatch(); // 清空缓存
                }
            }
            stat.executeBatch();
            stat.clearBatch();

            stat.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void test_insertAll_nbd() {
        try {
            // 加载驱动
            Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();

            // 创建连接
            Connection conn = DriverManager.getConnection(sourceURL1 + ";create=true");
            Statement stat = conn.createStatement();

            // stat.execute("drop table ol_itf_nbd_tourist_info");
            // // stat.execute("create table ol_itf_nbd_tourist_info (id int NOT NULL)");
            // // 插入数据

            String encoding = "GBK";
            File file = new File("C:/Users/zhangyang11/Desktop/test_data/ol_itf_nbd_tourist_info1.sql");
            if (file.isFile() && file.exists()) { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                int i = 0;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    i++;
                    stat.addBatch(lineTxt);
                    if (i % 1000 == 0) {
                        stat.executeBatch();
                        stat.clearBatch(); // 清空缓存
                    }
                }
                read.close();
            } else {
                System.out.println("找不到指定的文件");
            }
            stat.executeBatch();
            stat.clearBatch();

            stat.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void test_index_nbd() {
        Connection c = null;
        Statement stmt = null;
        try {
            // 加载驱动
            Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();

            // 创建连接
            c = DriverManager.getConnection(sourceURL1 + ";create=true");
            stmt = c.createStatement();
            long timebegin = System.currentTimeMillis();

            stmt.execute("create table ol_itf_nbd_tourist_info "
                    + "(id int NOT NULL, "
                    + " start_date varchar(10) NOT NULL DEFAULT '0000-00-00', "
                    + " start_city_key varchar(10) NOT NULL DEFAULT '其它', "
                    + " start_province_name varchar(60) NOT NULL DEFAULT '其它', "
                    + " target_city_key varchar(10) NOT NULL DEFAULT '其它', "
                    + " target_province_name varchar(40) NOT NULL DEFAULT '其它',"
                    + " order_nums int NOT NULL DEFAULT 0, "
                    + " pepole_nums int NOT NULL DEFAULT 0,"
                    + " sex_man_nums int NOT NULL DEFAULT 0, "
                    + " sex_female_nums int NOT NULL DEFAULT 0,"
                    + " adult_nums int NOT NULL DEFAULT 0, "
                    + " juveniles_nums int NOT NULL DEFAULT 0, "
                    + " nums_18_below int NOT NULL DEFAULT 0,"
                    + " nums_18_30 int NOT NULL DEFAULT 0,"
                    + " nums_30_40 int NOT NULL DEFAULT 0,"
                    + " nums_40_50 int NOT NULL DEFAULT 0,"
                    + " nums_50_over int NOT NULL DEFAULT 0, "
                    + " family_nums int NOT NULL DEFAULT 0, "
                    + " lovers_or_friends_nums int NOT NULL DEFAULT 0, "
                    + " agency_book_nums int NOT NULL DEFAULT 0, "
                    + " alone_nums int NOT NULL DEFAULT 0, "
                    + " other_tour_nums int NOT NULL DEFAULT 0, "
                    + " mobile_book_nums int NOT NULL DEFAULT 0, "
                    + " pc_book_nums int NOT NULL DEFAULT 0, "
                    + " other_book_nums int NOT NULL DEFAULT 0,"
                    + " col1 int NOT NULL DEFAULT 0,"
                    + " col2 int NOT NULL DEFAULT 0"
                    + ")");

            stmt.execute("create index pro_name_start_index on ol_itf_nbd_tourist_info(start_province_name,start_date)");
            long timeend = System.currentTimeMillis();

            System.out.println(timeend - timebegin + "ms");
            stmt.close();
            c.close();

            System.out.println("Operation done successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void test_query_nbd() {
        Connection c = null;
        Statement stmt = null;
        try {
            // 加载驱动
            Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();

            // 创建连接
            c = DriverManager.getConnection(sourceURL1 + ";create=true");
            stmt = c.createStatement();
            long timebegin = System.currentTimeMillis();

            ResultSet rs = stmt.executeQuery(
                    "select sum(pepole_nums) as aabb,start_province_name as bbaa from ol_itf_nbd_tourist_info where (start_province_name <>'') and (start_date>='2010-04-16' and start_date<='2017-05-16') and (target_province_name in('瑗胯棌','娌冲寳','鏂扮枂','鍙版咕','鍥涘窛','闄曡タ','灞辫タ','涓婃捣','灞变笢','闈掓捣','婢抽棬','瀹佸','鍐呰挋鍙�','鍚夋灄','姹熻タ','瀹夊窘','姹熻嫃','鍖椾含','婀栧崡','閲嶅簡','绂忓缓','婀栧寳','鐢樿們','棣欐腐','娌冲崡','骞夸笢','娴欐睙','骞胯タ','浜戝崡','榛戦緳姹�','璐靛窞','娴峰崡','杈藉畞')) and (start_date>='2010-05-01' and start_date<='2020-05-04') group by start_province_name order by sum(pepole_nums) desc");
            long timeend = System.currentTimeMillis();

            System.out.println(timeend - timebegin + "ms");

            while (rs.next()) {
                // System.out.println(rs);
                int id = rs.getInt("aabb");
                String name = rs.getString("bbaa");
                System.out.println("总人数 = " + id + "," + "来源地 = " + name);
                System.out.println("-----------------");
            }
            rs.close();
            stmt.close();
            c.close();

            System.out.println("Operation done successfully");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testH2_aa() {
        try {
            // 加载驱动

            // "org.apache.derby.jdbc.ClientDriver";org.apache.derby.jdbc.EmbeddedDriver
            Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();

            // 创建连接
            Connection conn = DriverManager.getConnection(sourceURL1 + ";create=true");
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

    public static void main(String[] args) {
        DerbyTest h2 = new DerbyTest();

        // h2.test_index_nbd();
        // h2.test_insertAll_nbd();

        h2.test_query_nbd();
        // h2.testH2_aa();

        // String aa =
        // "'西藏','河北','新疆','台湾','四川','陕西','山西','上海','山东','青海','澳门','宁夏','内蒙古','吉林','江西','安徽','江苏','北京','湖南','重庆','福建','湖北','甘肃','香港','河南','广东','浙江','广西','云南','黑龙江','贵州','海南','辽宁'";
        // try {
        // System.out.println(new String(aa.getBytes(), "GBK"));
        // } catch (UnsupportedEncodingException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
    }
}
