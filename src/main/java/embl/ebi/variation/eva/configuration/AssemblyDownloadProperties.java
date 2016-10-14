package embl.ebi.variation.eva.configuration;

import java.nio.file.Path;
import java.nio.file.Paths;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "assembly", ignoreUnknownFields = false)
public class AssemblyDownloadProperties {

    @NotNull
    @Pattern(regexp = "GCA_\\d+(\\.\\d+)?")
    private String accession;

    @NotNull
    @Size(min = 1)
    private String downloadRootPath;

    public AssemblyDownloadProperties() { }

    public AssemblyDownloadProperties(String accession, String downloadRootPath) {
        this.accession = accession;
        this.downloadRootPath = downloadRootPath;
    }

    public String getAccession() {
        return accession;
    }

    public void setAccession(String accession) {
        this.accession = accession;
    }

    public String getDownloadRootPath() {
        return downloadRootPath;
    }

    public void setDownloadRootPath(String downloadRootPath) {
        this.downloadRootPath = downloadRootPath;
    }

    public String getSequenceReportFileBasename() {
        return accession + "_sequence_report.txt";
    }

    public Path getDownloadPath() {
        return Paths.get(downloadRootPath, accession);
    }

    public Path getFastaDownloadPath() {
        return Paths.get(downloadRootPath, accession + ".fasta");
    }
}
