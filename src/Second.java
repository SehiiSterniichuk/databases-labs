import redis.clients.jedis.Jedis;

import java.io.File;
import java.util.List;
import java.util.Scanner;

public class Second {
    private static final String fileName = "Output.txt";
    private static final String key = "MyQueue";
    private static final String host = "localhost";
    private static final int port = 6379;
    private static final int timeout = 25;
    private static final FileManager fileManager = new FileManager();

    private static final File file = new File(fileName);

    public static List<String> pop(String key, Jedis jedis) {
        return jedis.brpop(timeout, key);//sadly timeout is ignored by library :(
    }

    public static void push(String key, String value, Jedis jedis) {
        jedis.lpush(key, value);
    }

    public static void write(File file, List<String> list) {
        fileManager.write(file, list.toString());
    }

    public static synchronized void print(String message) {
        System.out.println(message);
    }

    static boolean isWorking = true;
    static int sleepTimeForConsumer;
    static int sleepTimeForProducer;

    public static void main(String[] args) {
        fileManager.clearFile(file.getPath());
        try(var jedis = new Jedis(host, port)){
            jedis.del(key);
            try (final Scanner sc = new Scanner(System.in)) {
                System.out.print("Sleep time for producer: ");
                sleepTimeForProducer = sc.nextInt();
                System.out.print("Sleep time for consumer: ");
                sleepTimeForConsumer = sc.nextInt();
                System.out.print("Work time: ");
                final int timeOfWork = sc.nextInt();
                Thread producer = new Thread(Second::producer);
                Thread consumer = new Thread(Second::consumer);
                producer.start();
                consumer.start();
                sleep(timeOfWork);
                isWorking = false;
                join(consumer);
                join(producer);
            }
            var list = jedis.lrange(key, 0, -1);
            System.out.println("There is still list in the redis:");
            System.out.println(list);
        }
    }

    public static void consumer() {
        var jedis = new Jedis(host, port);
        while (isWorking) {
            sleep(sleepTimeForConsumer);
            var list = pop(key, jedis);
            print("Consumer has got from database: " + list.toString());
            write(file, list);
        }
    }


    public static void producer() {
        var jedis = new Jedis(host, port);
        while (isWorking) {
            sleep(sleepTimeForProducer);
            final String message = String.valueOf(System.currentTimeMillis());
            print("Producer has created message: " + message);
            push(key, message, jedis);
        }
    }

    private static void join(Thread thread){
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static void sleep(int sleepTime) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
