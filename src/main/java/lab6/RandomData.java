package lab6;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomData {
    public static final String[] names = {
            "Serhii",
            "Petro",
            "Bohdan",
            "Stanislav",
            "Vladislav",
            "Yulia",
            "Anastasia",
            "Viola",
            "Sophia",
            "Ira",
    };

    public static final String[] subject = {
            "Java",
            "C++",
            "Math",
            "Data Structure",
            "Discrete Math"
    };

    public static final Random rand = new Random();

    public String getRandomName() {
        int index = rand.nextInt(names.length);
        int randomNumber = rand.nextInt(10);
        return names[index] + randomNumber;
    }

    public String getRandomSubject() {
        int index = rand.nextInt(subject.length);
        return subject[index];
    }

    public String getRandomStudentAge() {
        return String.valueOf(17 + rand.nextInt(6));
    }

    public String getRandomWorkerAge() {
        return String.valueOf(19 + rand.nextInt(40));
    }

    public String getRandomWorkerSalary() {
        return String.valueOf(500 + rand.nextInt(5000));
    }

}
