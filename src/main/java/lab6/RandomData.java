package lab6;

import java.util.Random;

public class RandomData {
    public static final String[] NAMES = {
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

    public static final String[] SUBJECTS = {
            "Java",
            "C++",
            "Math",
            "DataBases",
            "Data Structure",
            "Discrete Math"
    };

    public static final String[] POSITIONS = {
            "Developer",
            "Designer",
            "Team Lead",
            "Manager",
            "HR",
            "CEO"
    };

    public static final Random rand = new Random();

    public String getRandomName() {
        int index = rand.nextInt(NAMES.length);
        int randomNumber = rand.nextInt(10);
        return NAMES[index] + randomNumber;
    }

    public String getRandomSubject() {
        int index = rand.nextInt(SUBJECTS.length);
        return SUBJECTS[index];
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

    public String getRandomWorkerPosition(){
        int index = rand.nextInt(POSITIONS.length);
        return POSITIONS[index];
    }

    public String getRandomAverageGrade(){
        return String.valueOf(60 + rand.nextInt(41));
    }

}
