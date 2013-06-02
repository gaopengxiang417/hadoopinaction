package com.gao.first;

import org.apache.hadoop.io.Text;

/**
 * User: wangchen.gpx
 * Date: 13-6-2
 * Time: 下午12:36
 */
public class TextTest {
    public static void strings() {
        String s = "\u0041\u00df\u6771\ud801\udc00";
        System.out.println(s.length());
        System.out.println(s.indexOf("\u0041"));
        System.out.println(s.indexOf("\u00df"));
        System.out.println(s.indexOf("\u6771"));
        System.out.println(s.indexOf("\ud801\udc00"));
    }

    public static void texts(){
        Text text = new Text("\u0041\u00df\u6771\ud801\udc00");
        System.out.println(text.getLength());
        System.out.println(text.find("\u0041"));
        System.out.println(text.find("\u00df"));
        System.out.println(text.find("\u6771"));
        System.out.println(text.find("\udc00"));
    }

    public static void main(String[] args) {
        strings();
        texts();
    }
}
