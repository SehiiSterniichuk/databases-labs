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
    public static final String PERSONAL_DATA = "personal data";
    public static final String PROFESSIONAL_DATA = "professional data";
    public static final String EDUCATION_DATA = "education data";
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
        System.setProperty("hadoop.home.dir", "/");
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config)) {
//          1 створення тестових двох таблиць і двох сімейств стовпців;
            MyTable worker = new MyTable(connection, WORKER_NAME, FAMILIES_OF_WORKER);
            MyTable student = new MyTable(connection, STUDENT_NAME, FAMILIES_OF_STUDENT);
            if (worker.exist() && student.exist()) {
                System.out.println("Tables are created");
            }
//          2 дозаписування даних у дві таблиці;
            int number = 3;
            putWorkers(worker, number);
            putStudents(student, number);

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

//            * 5 видалення даних з двох таблиць;
//            видалення рядків
            delete(worker, worker.getLastIndex());
            delete(student, student.getLastIndex());

//            видалення значень
            delete(worker, worker.getLastIndex(), PROFESSIONAL_DATA, SALARY);
            delete(student, student.getLastIndex(), EDUCATION_DATA, BEST_SUBJECT);

//          * 6 видалення таблиць.
            drop(worker);
            drop(student);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private static void putStudents(MyTable student, int number) {
        for (int i = 0; i < number; i++) {
            String studentName = rand.getRandomName();
            String studentAge = rand.getRandomStudentAge();
            String averageGrade = rand.getRandomAverageGrade();
            String bestSubject = rand.getRandomSubject();

            final String newRowStudent = String.valueOf(student.lastIndex() + 1);
            student.put(newRowStudent, PERSONAL_DATA, NAME, studentName);
            student.put(newRowStudent, PERSONAL_DATA, AGE, studentAge);
            student.put(newRowStudent, EDUCATION_DATA, AVERAGE_GRADE, averageGrade);
            student.put(newRowStudent, EDUCATION_DATA, BEST_SUBJECT, bestSubject);
        }
        checkSuccessfulPut(student, number);
    }

    @SuppressWarnings("DuplicatedCode")
    private static void putWorkers(MyTable worker, int number) {
        for (int i = 0; i < number; i++) {
            String workerName = rand.getRandomName();
            String workerAge = rand.getRandomWorkerAge();
            String workerSalary = rand.getRandomWorkerSalary();
            String workerPosition = rand.getRandomWorkerPosition();

            final String newRowWorker = String.valueOf(worker.lastIndex() + 1);
            worker.put(newRowWorker, PERSONAL_DATA, NAME, workerName);
            worker.put(newRowWorker, PERSONAL_DATA, AGE, workerAge);
            worker.put(newRowWorker, PROFESSIONAL_DATA, SALARY, workerSalary);
            worker.put(newRowWorker, PROFESSIONAL_DATA, POSITION, workerPosition);
        }
        checkSuccessfulPut(worker, number);
    }

    private static void checkSuccessfulPut(MyTable table, int number){
        if(table.count() >= number){
            System.out.printf("We have successfully put %d new %ss\n", number, table);
        }
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
        System.out.printf("\nNew %s of %s row in the %s:\t%s\n", column, row, table, value);
        table.put(row, family, column, value);
        var instanceOfWorker = table.get(row);
        System.out.printf("Updated %s instance:\n%s", table, instanceOfWorker);
    }

    private static void drop(MyTable table) {
        System.out.println("\nDrop table " + table);
        table.drop();
        System.out.printf("\n%s exists:\t%s\n\n", table, table.exist());
    }

    private static void delete(MyTable table, String row, String family, String column) {
        System.out.printf("Table row before deleting %s:%s\n", family, column);
        System.out.println(table.get(row));
        table.delete(row, family, column);
        System.out.printf("Table row after deleting %s:%s\n", family, column);
        System.out.println(table.get(row));
    }

    private static void delete(MyTable table, String row) {
        System.out.printf("\nRow %s of %s table is going to be deleted\n", row, table);
        table.delete(row);
        System.out.println("Deleted\nCheck the table");
        scan(table);
    }
}