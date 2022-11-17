package lab6;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomData {
    private static final String[] names = {
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

    private static final String[] subject = {
            "Java",
            "C++",
            "Math",
            "Data Structure",
            "Discrete Math"
    };

    private static final Random rand = new Random();

    private String getRandomName() {
        int index = rand.nextInt(names.length);
        int randomNumber = rand.nextInt(10);
        return names[index] + randomNumber;
    }

    private String getRandomSubject() {
        int index = rand.nextInt(subject.length);
        return subject[index];
    }

    private String getRandomStudentAge() {
        return String.valueOf(17 + rand.nextInt(6));
    }

    private String getRandomWorkerAge() {
        return String.valueOf(19 + rand.nextInt(40));
    }

    private String getRandomWorkerSalary() {
        return String.valueOf(500 + rand.nextInt(5000));
    }
}
