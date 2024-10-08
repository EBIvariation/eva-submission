import java.io.FileWriter;
import java.io.IOException;


public class FakeRemappingExtractionPipeline {
    
    public static void main(String[] args) {
        String outString = "java -jar extraction.jar";
        String inFile = null;
        for (String arg: args) {
            outString += " " + arg;
            if (arg.startsWith("--parameters.fasta="))
            inFile = arg.substring("--parameters.fasta=".length(), arg.length()-"_custom.fa".length());
        }
        System.out.println(outString);
        System.out.println(inFile);

        // real pipeline gets this from properties
	    String outFile2 =  inFile + "_eva.vcf";
        try {
            FileWriter writer = new FileWriter(outFile2);
            writer.write("remapped eva variants\n");
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
	    }
    }

}
