
public class FakeVariantLoadPipeline {
    
    public static void main(String[] args) {
	String outString = "java -jar variant-load.jar";
	for (String arg: args) {
	    outString += " " + arg;
	}
	System.out.println(outString);
    }

}
