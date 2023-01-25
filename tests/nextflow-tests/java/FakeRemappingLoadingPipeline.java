
public class FakeRemappingLoadingPipeline {
    
    public static void main(String[] args) {
        String outString = "java -jar remap-loading.jar";
        for (String arg: args) {
            outString += " " + arg;
        }
        System.out.println(outString);
    }

}
