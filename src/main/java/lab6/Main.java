package lab6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Scanner;

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
    public static final Scanner sc = new Scanner(System.in);

    public static void main(String[] args) {
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config)) {
//          1 створення тестових двох таблиць і двох сімейств стовпців;
            MyTable worker = new MyTable(connection, WORKER_NAME, FAMILIES_OF_WORKER);
            MyTable student = new MyTable(connection, STUDENT_NAME, FAMILIES_OF_STUDENT);
//          2 дозаписування даних у дві таблиці;
            String workerName = rand.getRandomName();
            String workerAge = rand.getRandomWorkerAge();
            String workerSalary = rand.getRandomWorkerSalary();
            String workerPosition = rand.getRandomWorkerPosition();
            System.out.printf("""
                            Data will be stored in worker table:
                            name: %s\tage: %s\tsalary: %s\tposition: %s
                            """,
                    workerName, workerAge, workerSalary, workerPosition);

            final String countWorker = String.valueOf(worker.count() + 1);
            worker.put(countWorker, PERSONAL_DATA, NAME, workerName);
            worker.put(countWorker, PERSONAL_DATA, AGE, workerAge);
            worker.put(countWorker, PROFESSIONAL_DATA, SALARY, workerSalary);
            worker.put(countWorker, PROFESSIONAL_DATA, POSITION, workerPosition);

            String studentName = rand.getRandomName();
            String studentAge = rand.getRandomStudentAge();
            String averageGrade = rand.getRandomAverageGrade();
            String bestSubject = rand.getRandomSubject();
            System.out.printf("""
                            Data will be stored in student table:
                            name: %s\tage: %s\tgrade: %s\tbest subject: %s
                            """,
                    studentName, studentAge, averageGrade, bestSubject);

            final String countStudent = String.valueOf(student.count() + 1);
            student.put(countStudent, PERSONAL_DATA, NAME, studentName);
            student.put(countStudent, PERSONAL_DATA, AGE, studentAge);
            student.put(countStudent, EDUCATION_DATA, AVERAGE_GRADE, averageGrade);
            student.put(countStudent, EDUCATION_DATA, BEST_SUBJECT, bestSubject);
//          3 зчитування даних з двох таблиць;

            //сканування
            scan(worker);
            scan(student);
            // get запит
            get(worker);
            get(student);

//          * 4 оновлення даних у двох таблицях;
            updateLastInstance(worker, PROFESSIONAL_DATA, SALARY, rand.getRandomWorkerSalary());
            updateLastInstance(student, EDUCATION_DATA, AVERAGE_GRADE, rand.getRandomAverageGrade());

//            * 5 видалення даних з двох таблиць;
            System.out.print("Do you want to delete some rows from hbase?\nYes - 1\nEnter:");
            if ("1".equals(sc.nextLine())) {
                System.out.print("Which row from worker do you want to delete?\nEnter:");
                String deleteRow = sc.nextLine();
                worker.delete(deleteRow);
                System.out.println(worker.scan());
                System.out.print("Which row from student do you want to delete?\nEnter:");
                deleteRow = sc.nextLine();
                student.delete(deleteRow);
                System.out.println(student.scan());
            }
            System.out.print("Do you want to delete some specific cells from hbase?\nYes - 1\nEnter:");
            if ("1".equals(sc.nextLine())) {
                System.out.print("Which row from worker do you want to manage?\nEnter:");
                String row = sc.nextLine();
                System.out.println(worker.get(row));
                System.out.print("Which column family?\nEnter:");
                String family = sc.nextLine();
                System.out.print("Which column?\nEnter:");
                String column = sc.nextLine();
                worker.delete(row, family, column);
                System.out.println(worker.scan());


                System.out.print("Which row from student do you want to delete?\nEnter:");
                row = sc.nextLine();
                System.out.println(worker.get(row));
                System.out.print("Which column family?\nEnter:");
                family = sc.nextLine();
                System.out.print("Which column?\nEnter:");
                column = sc.nextLine();
                worker.delete(row, family, column);
                System.out.println(worker.scan());
            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static void scan(MyTable table) {
        System.out.println("\nScan result of " + table);
        String scan = table.scan();
        System.out.println(scan);
    }

    public static void get(MyTable table) {
        System.out.println("Get result of " + table);
        String lastInstanceRowOfTable = String.valueOf(table.count());
        String instance = table.get(lastInstanceRowOfTable);
        System.out.println(instance);
    }

    public static void updateLastInstance(MyTable table, String family, String column, String value) {
        String lastInstanceRowOfWorker = String.valueOf(table.count());
        update(table, lastInstanceRowOfWorker, family, column, value);
    }

    public static void update(MyTable table, String row, String family, String column, String value) {
        System.out.printf("New " + column + " of " + row + "row in the " + table + ":\t%s\n", value);
        table.put(row, family, column, value);
        var instanceOfWorker = table.get(row);
        System.out.printf("Updated " + table + " instance:\n%s", instanceOfWorker);
    }
}