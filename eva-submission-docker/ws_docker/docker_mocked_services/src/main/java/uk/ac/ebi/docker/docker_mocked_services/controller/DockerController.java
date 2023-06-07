package uk.ac.ebi.docker.docker_mocked_services.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@RequestMapping("/")
@RestController
public class DockerController {
    public static final Map<String, String> enaHoldDateMap = new HashMap<>();

    public DockerController() {
    }

    /* To get a mocked hold date value for a projectAlias*/
    @PostMapping(value = "mocked_ena/submit/drop-box/submit/", consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = "application/xml")
    public ResponseEntity<String> getMockedENAHoldDate(@RequestPart("SUBMISSION") MultipartFile xmlFile) {
        String projectAlias = getProjectAliasFromRequest(xmlFile);

        if (projectAlias == null) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Could not get project alias from input xml");
        }

        if (enaHoldDateMap.containsKey(projectAlias)) {
            return ResponseEntity.of(Optional.of(getENAHoldTemplateXMLWithDate(enaHoldDateMap.get(projectAlias))));
        } else {
            String holdDate = LocalDateTime.now().plusDays(10).toString();
            return ResponseEntity.of(Optional.of(getENAHoldTemplateXMLWithDate(holdDate)));
        }
    }

    /* To set a mocked value for a projectAlias*/
    @PutMapping(value = "mocked_ena/submit/drop-box/submit/", produces = "application/xml")
    public ResponseEntity<String> setMockedENAHoldDate(@RequestBody String xmlRequest) {
        try {
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(new InputSource(new StringReader(xmlRequest)));

            String projectAlias = doc.getElementsByTagName("PROJECT").item(0).getAttributes().getNamedItem("alias").getNodeValue();
            String hold_date = doc.getElementsByTagName("PROJECT").item(0).getAttributes().getNamedItem("holdUntilDate").getNodeValue();

            enaHoldDateMap.put(projectAlias, hold_date);
            return ResponseEntity.ok().build();

        } catch (ParserConfigurationException | IOException | SAXException pe) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(pe.toString());
        }

    }

    /* To set a mocked value for a projectAlias*/
    @DeleteMapping(value = "mocked_ena/submit/drop-box/submit/")
    public ResponseEntity<String> delMockedENAHoldDate(@RequestBody String projectAlias) {
        enaHoldDateMap.remove(projectAlias);
        return ResponseEntity.ok().build();
    }


    private String getENAHoldTemplateXMLWithDate(String hold_date) {
        return String.format("<PROJECT accession=\"PRJEB99999\" alias=\"Golden Eagles\" status=\"PRIVATE\" holdUntilDate=\"%s\"/>", hold_date);
    }


    private String getProjectAliasFromRequest(MultipartFile xmlFile) {
        try {
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(xmlFile.getInputStream());

            return doc.getElementsByTagName("RECEIPT").item(0).getAttributes().getNamedItem("target").getNodeValue();

        } catch (ParserConfigurationException | IOException | SAXException pe) {
            return null;
        }
    }


}
