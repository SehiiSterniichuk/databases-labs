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
            System.out.println("\nScan result of worker");
            String scanOfWorker = worker.scan();
            System.out.println(scanOfWorker);

            System.out.println("Scan result of student");
            String scanOfStudent = student.scan();
            System.out.println(scanOfStudent);
            // get запит
            System.out.println("Get result of worker");
            String lastInstanceRowOfWorker = String.valueOf(worker.count());
            String instanceOfWorker = worker.get(lastInstanceRowOfWorker);
            System.out.println(instanceOfWorker);

            System.out.println("Get result of student");
            String lastInstanceRowOfStudent = String.valueOf(student.count());
            String instanceOfStudent = worker.get(lastInstanceRowOfStudent);
            System.out.println(instanceOfStudent);

//          * 4 оновлення даних у двох таблицях;
            var newSalaryOfLastWorker = rand.getRandomWorkerSalary();
            System.out.printf("New salary of last worker:\t%s\n", newSalaryOfLastWorker);
            worker.put(countWorker, PROFESSIONAL_DATA, SALARY, newSalaryOfLastWorker);
            instanceOfWorker = worker.get(lastInstanceRowOfWorker);
            System.out.printf("Updated worker instance:\n%s", instanceOfWorker);

            var newAverageGradeOfStudent = rand.getRandomAverageGrade();
            System.out.printf("\nNew average grade of last student:\t%s\n", newAverageGradeOfStudent);
            student.put(countStudent, EDUCATION_DATA, AVERAGE_GRADE, newAverageGradeOfStudent);
            instanceOfStudent = student.get(lastInstanceRowOfStudent);
            System.out.printf("Updated student instance:\n%s", instanceOfStudent);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
