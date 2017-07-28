package com.zy.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <Description>文件测试 <br>
 * 
 * @author zhangyang11<br>
 * @version 1.0<br>
 * @taskId <br>
 * @CreateDate 2017年5月17日 <br>
 */
public class FileUtils {

    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

    public static String readFileToString(String fileName) {

        StringBuffer result = new StringBuffer();

        try {
            String encoding = "GBK";
            File file = new File(fileName);
            if (file.isFile() && file.exists()) { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    result.append(lineTxt);
                }
                read.close();
            } else {
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            logger.error(e.getMessage());
        }

        return result.toString();
    }

    public static List<String> readFileToStringArray(String fileName) {

        List<String> lineStrArray = new ArrayList<>();

        try {
            String encoding = "GBK";
            File file = new File(fileName);
            if (file.isFile() && file.exists()) { // 判断文件是否存在
                InputStreamReader read = new InputStreamReader(
                        new FileInputStream(file), encoding);// 考虑到编码格式
                BufferedReader bufferedReader = new BufferedReader(read);
                String lineTxt = null;
                while ((lineTxt = bufferedReader.readLine()) != null) {
                    lineStrArray.add(lineTxt);
                }
                read.close();
            } else {
                System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
            System.out.println("读取文件内容出错");
            logger.error(e.getMessage());
        }

        return lineStrArray;
    }

    public static void main(String[] args) {
        FileUtils.readFileToString("D:/test.txt");
    }
}
