package embl.ebi.variation.eva.seqrep_fasta_dl;

import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * Created by tom on 08/08/16.
 */
@Component
public class SequenceReportPathTransformer {

    public String transform(Message message){
        String remoteDirectory = (String) message.getHeaders().get("file_remoteDirectory");
        String remoteFile = (String) message.getPayload();

        String remoteFilepath = String.format("%s/%s", remoteDirectory, remoteFile);

        return remoteFilepath;
    }

}
