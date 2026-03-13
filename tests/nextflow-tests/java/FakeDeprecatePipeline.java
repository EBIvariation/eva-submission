
public class FakeDeprecatePipeline {

    public static void main(String[] args) {
        String outString = "java -jar deprecate.jar";
        for (String arg: args) {
            outString += " " + arg;
        }
        System.out.println(outString);
    }

}
