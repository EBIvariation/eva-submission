package embl.ebi.variation.eva.fasta_download;

import org.springframework.integration.annotation.Splitter;
import org.springframework.integration.annotation.Transformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tom on 18/08/16.
 */
@Component
public class SplitterConfig {

    @Splitter(inputChannel = "channelToSplitter", outputChannel = "channelToHttpRequest")
    public List<Message> getChromosomeAccessionsFromFile(Message<File> msg){
        List<Message> messageList = new ArrayList<>();
        List<String> chromAccList = (List<String>) msg.getPayload();
        chromAccList.forEach(chromAcc -> messageList.add(new GenericMessage(chromAcc)));
        return messageList;
    }
}
