package lab6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/*
* Встановіть бібліотеку мови програмування на ваш вибір і реалізуйте
функції:
* створення тестових двох таблиць і двох сімейств стовпців;
* дозаписування даних у дві таблиці;
* зчитування даних з двох таблиць;
* оновлення даних у двох таблицях;
* видалення даних з двох таблиць;
* видалення таблиць.
* */
public class Main {

    public static final String WORKER_NAME = "worker";
    public static final String STUDENT_NAME = "student";
    public static final String PERSONAL_DATA = "Personal data";
    public static final String PROFESSIONAL_DATA = "Professional data";
    public static final String EDUCATION_DATA = "Education data";
    public static final String NAME = "name";
    public static final String AGE = "age";
    public static final String POSITION = "position";
    public static final String SALARY = "salary";
    public static final String AVERAGE_GRADE = "average grade";
    public static final String BEST_SUBJECT = "best subject";
    public static final String[] FAMILIES_OF_WORKER = {PERSONAL_DATA, PROFESSIONAL_DATA};
    public static final String[] FAMILIES_OF_STUDENT = {PERSONAL_DATA, EDUCATION_DATA};
    public static final RandomData rand = new RandomData();

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config)) {
//          * створення тестових двох таблиць і двох сімейств стовпців;
            MyTable worker = new MyTable(connection, WORKER_NAME, FAMILIES_OF_WORKER);
            MyTable student = new MyTable(connection, STUDENT_NAME, FAMILIES_OF_STUDENT);
//          * дозаписування даних у дві таблиці;
            final String countWorker = String.valueOf(worker.count() + 1);
            worker.put(countWorker, PERSONAL_DATA, NAME, rand.getRandomName());
            worker.put(countWorker, PERSONAL_DATA, AGE, rand.getRandomWorkerAge());

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
