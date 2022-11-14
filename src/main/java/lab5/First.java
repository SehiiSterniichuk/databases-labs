package lab5;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;



import java.util.Scanner;

public class First {
    private static final String key = "MyKey";
    private static final String host = "localhost";
    private static final int port = 6379;

    public static void main(String[] args) {
        Response<Long> value;
        try (Jedis redis = new Jedis(host, port)) {
            Transaction multi = redis.multi();
            System.out.print("Enter your number: ");
            int input;
            try (Scanner sc = new Scanner(System.in)) {
                input = sc.nextInt();
            }
            multi.set(key, String.valueOf(input));
            value = multi.incr(key);
            multi.exec();
        }
        System.out.printf("Result: %s", value.get());
    }
}