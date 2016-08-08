package embl.ebi.variation.eva;


import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 08/08/16.
 */
public class StringSplitter {

    public List<String> splitOnCommaSpace(String inputString){
        String[] splits = inputString.split(",");
        List<String> list = new ArrayList<String>();
        for (String split : splits) {
            String trimmed = split.trim();
            if (trimmed.length() > 0) {
                list.add(trimmed);
            }
        }
        return list;
    }

}
