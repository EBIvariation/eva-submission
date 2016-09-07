package embl.ebi.variation.eva.fastadownload;

import org.junit.Test;
import org.springframework.messaging.support.GenericMessage;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tom on 23/08/16.
 */
public class SequenceReportPathTransformerTest {
    @Test
    public void transform() throws Exception {
        SequenceReportPathTransformer sequenceReportPathTransformer = new SequenceReportPathTransformer();

        for (String remoteDirectory : this.getFileRemoteDirectories()) {
            for (String remoteFile : this.getRemoteFiles()){
                Map<String, Object> headers = new HashMap<>();
                headers.put("file_remoteDirectory", remoteDirectory);
                GenericMessage message = new GenericMessage<String>(remoteFile, headers);
                assert(sequenceReportPathTransformer.transform(message).equals(Paths.get(remoteDirectory, remoteFile).toString()));
            }
        }

    }

    private String[] getFileRemoteDirectories() {
        String[] fileRemoteDirectories = {
                "pub/databases/ena/assembly/GCA_000/GCA_000001/",
                "",
                "pub/databases/ena/assembly/GCA_000/GCA_000001/pub/databases/ena/assembly/GCA_000/GCA_000001/",
                "////////////",
                "pub/databases/ena/assembly/GCA_000//GCA_000001/",
                "/pub/databases/ena/assembly/GCA_000/GCA_000001/"
        };

        return fileRemoteDirectories;
    }

    private String[] getRemoteFiles() {
        String[] remoteFiles = {
                "GCA_000001215.2_sequence_report.txt",
                "",
                "GCA_000001215.2_sequence_report.txtGCA_000001215.2_sequence_report.txt",
                "////////////",
                "GCA_000001215.2",
                "/GCA_000001215.2_sequence_report.txt"
        };

        return remoteFiles;
    }

}