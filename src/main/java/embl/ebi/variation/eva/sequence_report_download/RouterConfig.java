package embl.ebi.variation.eva.sequence_report_download;

import org.springframework.stereotype.Component;

import java.io.File;

/**
 * Created by tom on 19/08/16.
 */
@Component
public class RouterConfig {

    public String seqReportRouter(String filePath){
        if (new File(filePath).exists()) {
            return "channelIntoDownloadSeqRep";
        } else {
            return "channelIntoDownloadFasta";
        }
    }

    public String fastaRouter(String filePath){
        if (new File(filePath).exists()) {
            return "channelIntoDownloadFasta";
        } else {
            return "testChannel";
        }
    }
}
