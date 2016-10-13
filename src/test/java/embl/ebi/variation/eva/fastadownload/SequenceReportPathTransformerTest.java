package embl.ebi.variation.eva.fastadownload;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;
import org.springframework.messaging.support.GenericMessage;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tom on 23/08/16.
 */
public class SequenceReportPathTransformerTest {

    private static SequenceReportPathTransformer sequenceReportPathTransformer;

    @BeforeClass
    public static void setUp() {
        sequenceReportPathTransformer = new SequenceReportPathTransformer();
    }

    @Test
    public void transformTestAbsolutePath() throws Exception {
        String fileName = "GCA_000001215.2_sequence_report.txt";

        String[] absolutePaths = {
                "/pub/databases/ena/assembly/GCA_000/GCA_000001/",
                "/pub/databases/ena/assembly/GCA_102/GCA_000001/",
                "/pub/databases/ena/assembly/GCA_001/GCA_99999/",
                "/pub/databases/ena/assembly/GCA_000/GCA_000001/"
        };

        for (String remoteDirectory : absolutePaths) {
            GenericMessage message = buildMessageHelper(remoteDirectory, fileName);
            Assert.assertEquals(sequenceReportPathTransformer.transform(message), Paths.get(remoteDirectory, fileName).toString());
        }
    }

    @Test
    public void transformTestRelativePath() throws Exception {
        String fileName = "GCA_000001215.2_sequence_report.txt";

        String[] relativePaths = {
                "pub/databases/ena/assembly/GCA_000/GCA_000001/",
                "pub/databases/ena/assembly/GCA_102/GCA_000001/",
                "pub/databases/ena/assembly/GCA_001/GCA_99999/",
                "pub/databases/ena/assembly/GCA_000/GCA_000001/"
        };

        for (String remoteDirectory : relativePaths) {
            GenericMessage message = buildMessageHelper(remoteDirectory, fileName);
            Assert.assertEquals(sequenceReportPathTransformer.transform(message), Paths.get(remoteDirectory, fileName).toString());
        }
    }

    @Test
    public void transformTestUnusualPath() throws Exception {
        String fileName = "GCA_000001215.2_sequence_report.txt";

        String[] unusualPaths = {
                "",
                "////////////",
                "apath",
                " ",
                fileName
        };

        for (String remoteDirectory : unusualPaths) {
            GenericMessage message = buildMessageHelper(remoteDirectory, fileName);
            Assert.assertEquals(sequenceReportPathTransformer.transform(message), Paths.get(remoteDirectory, fileName).toString());
        }
    }

    @Test
    public void transformTestUnusualFilename() throws Exception {
        String remoteDirectory = "pub/databases/ena/assembly/GCA_000/GCA_000001/";

        String[] unusualFileNames = {
                "",
                " ",
                ".filename",
                "a file name",
                remoteDirectory
        };

        for (String fileName : unusualFileNames) {
            GenericMessage message = buildMessageHelper(remoteDirectory, fileName);
            Assert.assertEquals(sequenceReportPathTransformer.transform(message), Paths.get(remoteDirectory, fileName).toString());
        }
    }

    private GenericMessage buildMessageHelper(String remoteDirectory, String remoteFile) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("file_remoteDirectory", remoteDirectory);
        return new GenericMessage<String>(remoteFile, headers);
    }

}