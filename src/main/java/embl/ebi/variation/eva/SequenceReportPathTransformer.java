package embl.ebi.variation.eva;

import org.springframework.messaging.Message;

/**
 * Created by tom on 08/08/16.
 */
public class SequenceReportPathTransformer {

    public String transform(Message message){
        String remoteDirectory = (String) message.getHeaders().get("file_remoteDirectory");
        String remoteFile = (String) message.getPayload();

        String remoteFilepath = String.format("%s/%s", remoteDirectory, remoteFile);

        return remoteFilepath;
    }

}
