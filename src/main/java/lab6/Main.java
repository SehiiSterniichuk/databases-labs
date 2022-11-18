package lab6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/*
* Встановіть бібліотеку мови програмування на ваш вибір і реалізуйте
функції:
* 1 створення тестових двох таблиць і двох сімейств стовпців;
* 2 дозаписування даних у дві таблиці;
* 3 зчитування даних з двох таблиць;
* 4 оновлення даних у двох таблицях;
* 5 видалення даних з двох таблиць;
* 6 видалення таблиць.
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
//          1 створення тестових двох таблиць і двох сімейств стовпців;
            MyTable worker = new MyTable(connection, WORKER_NAME, FAMILIES_OF_WORKER);
            MyTable student = new MyTable(connection, STUDENT_NAME, FAMILIES_OF_STUDENT);
//          2 дозаписування даних у дві таблиці;
            task2(worker, student);
//          3 зчитування даних з двох таблиць;

            //сканування
            scan(worker);
            scan(student);
            // get запит
            get(worker, worker.getLastIndex());
            get(student, student.getLastIndex());

//          * 4 оновлення даних у двох таблицях;
            update(worker, worker.getLastIndex(), PROFESSIONAL_DATA, SALARY, rand.getRandomWorkerSalary());
            update(student, student.getLastIndex(), EDUCATION_DATA, AVERAGE_GRADE, rand.getRandomAverageGrade());
            scan(worker);
            scan(student);

//            * 5 видалення даних з двох таблиць;
//            видалення рядків
            worker.delete(worker.getLastIndex());
            student.delete(student.getLastIndex());
            scan(worker);
            scan(student);

//            видалення значень
            worker.delete(worker.getLastIndex(), PROFESSIONAL_DATA, SALARY);
            student.delete(student.getLastIndex(), EDUCATION_DATA, BEST_SUBJECT);
            scan(worker);
            scan(student);

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private static void printNewData(String tableName, String... data) {
        var builder = new StringBuilder(String.format("Data will be stored in %s table:\n", tableName));
        for (int i = 0; i + 1 < data.length; i += 2) {
            String column = data[i];
            String value = data[i + 1];
            builder.append(String.format("%s: %s\t", column, value));
        }
        builder.append('\n');
        System.out.println(builder);
    }


    @SuppressWarnings("DuplicatedCode")
    private static void task2(MyTable worker, MyTable student) {
        String workerName = rand.getRandomName();
        String workerAge = rand.getRandomWorkerAge();
        String workerSalary = rand.getRandomWorkerSalary();
        String workerPosition = rand.getRandomWorkerPosition();
        printNewData(worker.toString(), workerName, workerAge, workerSalary, workerPosition);

        final String newRowWorker = String.valueOf(worker.lastIndex() + 1);
        worker.put(newRowWorker, PERSONAL_DATA, NAME, workerName);
        worker.put(newRowWorker, PERSONAL_DATA, AGE, workerAge);
        worker.put(newRowWorker, PROFESSIONAL_DATA, SALARY, workerSalary);
        worker.put(newRowWorker, PROFESSIONAL_DATA, POSITION, workerPosition);

        String studentName = rand.getRandomName();
        String studentAge = rand.getRandomStudentAge();
        String averageGrade = rand.getRandomAverageGrade();
        String bestSubject = rand.getRandomSubject();
        printNewData(student.toString(), studentName, studentAge, averageGrade, bestSubject);

        final String newRowStudent = String.valueOf(student.lastIndex() + 1);
        student.put(newRowStudent, PERSONAL_DATA, NAME, studentName);
        student.put(newRowStudent, PERSONAL_DATA, AGE, studentAge);
        student.put(newRowStudent, EDUCATION_DATA, AVERAGE_GRADE, averageGrade);
        student.put(newRowStudent, EDUCATION_DATA, BEST_SUBJECT, bestSubject);
    }

    public static void scan(MyTable table) {
        System.out.println("\nScan result of " + table);
        String scan = table.scan();
        System.out.println(scan);
    }

    public static void get(MyTable table, String row) {
        System.out.println("Get result of " + table);
        String instance = table.get(row);
        System.out.println(instance);
    }

    public static void update(MyTable table, String row, String family, String column, String value) {
        System.out.printf("New %s of %s row in the %s:\t%s\n", column, row, table, value);
        table.put(row, family, column, value);
        var instanceOfWorker = table.get(row);
        System.out.printf("Updated %s instance:\n%s", table, instanceOfWorker);
    }
}