import java.io.FileWriter;
import java.io.IOException;


public class FakeAccessionPipeline {
    
    public static void main(String[] args) {
	String outString = "java -jar accession.jar";
	String inFile = null;
	for (String arg: args) {
	    outString += " " + arg;
	    if (arg.startsWith("--spring.config.location=")){
	        inFile = arg.substring("--spring.config.location=file:".length());
	    }
	}
	System.out.println(outString);

	// real pipeline gets this from properties
	String outFile = "../../../project/public/" + inFile.substring(0, inFile.indexOf(".")) + ".accessioned.vcf";
	try {
	    FileWriter writer = new FileWriter(outFile);
	    writer.write("accessioned vcf\n");
	    writer.close();
	} catch (IOException e) {
	    e.printStackTrace();
	}
    }

}
