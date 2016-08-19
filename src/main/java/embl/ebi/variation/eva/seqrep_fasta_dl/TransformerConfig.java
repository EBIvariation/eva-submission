package embl.ebi.variation.eva.seqrep_fasta_dl;

import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.stereotype.Component;

/**
 * Created by tom on 19/08/16.
 */
@Component
public class TransformerConfig {

    public Message<String> changePayloadForEnaFtpSeqRepDir(Message<String> msg){
        return new GenericMessage<String>((String) msg.getHeaders().get("enaFtpSeqRepDir"), msg.getHeaders());
    }

    public Message<String> changePayloadForSeqReportLocalPath(Message<String> msg){
        return new GenericMessage<String>((String) msg.getHeaders().get("seqReportLocalPath"), msg.getHeaders());
    }
}
