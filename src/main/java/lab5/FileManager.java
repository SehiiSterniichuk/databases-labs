package lab5;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileManager {
    static final boolean append = true;

    public void clearFile(String path){
        //noinspection EmptyTryBlock
        try (var ignored = new FileWriter(path)){
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(File file, String text) {
        try (var myWriter = new FileWriter(file, append)) {
            myWriter.write(text + '\n');
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
