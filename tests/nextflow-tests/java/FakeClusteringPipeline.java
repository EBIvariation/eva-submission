import java.io.FileWriter;
import java.io.IOException;

public class FakeClusteringPipeline {
    
    public static void main(String[] args) {
        String outString = "java -jar clustering.jar";
        for (String arg: args) {
            outString += " " + arg;
        }
         String inFile = null;
        for (String arg: args) {
            outString += " " + arg;
            if (arg.startsWith("--spring.config.location="))
            inFile = arg.substring("--spring.config.location".length(), arg.length()-".properties".length());
        }
        System.out.println(outString);
        System.out.println(inFile);

        // real pipeline gets this from properties
	    String outFile1 =  System.getProperty("user.dir") + "/GCA_0000003_rs_report.txt";
        System.out.println(outFile1);

        try {
            FileWriter writer = new FileWriter(outFile1);
            writer.write("clustered variants\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
	    }
    }

}
