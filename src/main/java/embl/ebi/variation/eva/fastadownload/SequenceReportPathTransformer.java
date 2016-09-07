package embl.ebi.variation.eva.fastadownload;

import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;

/**
 * Created by tom on 23/08/16.
 */
@Component
public class SequenceReportPathTransformer {

    public String transform(Message message){
        String remoteDirectory = (String) message.getHeaders().get("file_remoteDirectory");
        String remoteFile = (String) message.getPayload();
        String remoteFilepath = Paths.get(remoteDirectory, remoteFile).toString();
        return remoteFilepath;
    }

}
