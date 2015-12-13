package br.com.rodrigoma.poc_kafka;

import avro.generate.AccessLogLine;

import java.util.Date;
import java.util.Random;

public class EventGenerator {

    static int numUsers = 10;
    static String[] websites = {"support.html","about.html","foo.html", "bar.html", "home.html", "search.html", "list.html", "help.html", "bar.html", "foo.html"};

    public static AccessLogLine getNext() {
        Random r = new Random();

        return AccessLogLine.newBuilder()
                .setIp("192.168.0."+ r.nextInt(numUsers))
                .setReferrer("www.example.com")
                .setTimestamp(new Date().getTime())
                .setUrl(websites[r.nextInt(websites.length)])
                .setUseragent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36")
                .build();
    }
}