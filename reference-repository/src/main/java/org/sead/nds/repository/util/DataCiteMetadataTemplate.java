package org.sead.nds.repository.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/* Adapted from import edu.harvard.iq.dataverse.DataCiteMetadataTemplate */

public class DataCiteMetadataTemplate {

    private static final Logger logger = LogManager.getLogger(DataCiteMetadataTemplate.class);
    private static String template;

    public static final String title = "${title}";
    public static final String publisher = "${publisher}";
    public static final String publicationYear = "${publicationYear}";
    public static final String description = "${description}";
    public static final String creators = "${creators}";
    public static final String contributors = "${contributors}";
    public static final String relatedIdentifiers = "${relatedIdentifiers}";
    public static final String contactType = "ContactPerson";
    public static final String producerType = "Producer";
    public static final String identifier = "${identifier}";

    static {
        try (InputStream in = DataCiteMetadataTemplate.class.getResourceAsStream("/datacite_metadata_template.xml")) {
            template = Util.readAndClose(in, "utf-8");
        } catch (Exception e) {
            logger.log(Level.ERROR, "datacite metadata template load error");
            logger.log(Level.ERROR, "String " + e.toString());
            logger.log(Level.ERROR, "localized message " + e.getLocalizedMessage());
            logger.log(Level.ERROR, "cause " + e.getCause());
            logger.log(Level.ERROR, "message " + e.getMessage());
        }
    }

    private String xmlMetadata;

    private String datasetIdentifier;
    private List<String> datafileIdentifiers;

    public DataCiteMetadataTemplate() {
    }

    public List<String> getDatafileIdentifiers() {
        return datafileIdentifiers;
    }

    public void setDatafileIdentifiers(List<String> datafileIdentifiers) {
        this.datafileIdentifiers = datafileIdentifiers;
    }

    public DataCiteMetadataTemplate(Map<String, String> metadata) {
        resetMetadata(metadata);
    }

    public void resetMetadata(Map<String, String> metadata) {
        xmlMetadata = template;
        for (String key : metadata.keySet()) {
            xmlMetadata = xmlMetadata.replace(key, metadata.get(key));
        }
    }

    public String getMetadata() {
        return xmlMetadata;
    }

    // xmlMetadata = template.replace("${identifier}", this.identifier.trim())

    public static String generateCreatorsXml(JSONArray people) {
        StringBuilder creatorsElement = new StringBuilder("");
        for (int i = 0; i < people.length(); i++) {
            Object o = people.get(i);
            if (o instanceof String) {
                creatorsElement.append("<creator><creatorName>");
                creatorsElement.append((String) o);
                creatorsElement.append("</creatorName></creator>");
            } else if (o instanceof JSONObject) {
                JSONObject jo = (JSONObject) o;
                creatorsElement.append("<creator><creatorName>");
                creatorsElement.append(jo.getString("givenName") + " "
                        + jo.getString("familyName"));
                creatorsElement.append("</creatorName>");
                creatorsElement.append("<nameIdentifier schemeURI=\"https://orcid.org/\" nameIdentifierScheme=\"ORCID\">" + jo.getString("@id") + "</nameIdentifier></creator>");
            }
        }
        return creatorsElement.toString();
    }

    public static String generateContributorsXml(String type, JSONArray people) {
        StringBuilder contributorsElement = new StringBuilder("");
        if (people != null) {
            for (int i = 0; i < people.length(); i++) {
                Object o = people.get(i);
                if (o instanceof String) {
                    contributorsElement.append("<contributor contributorType=\"" + type + "\"><contributorName>");
                    contributorsElement.append((String) o);
                    contributorsElement.append("</contributorName></contributor>");
                } else if (o instanceof JSONObject) {
                    JSONObject jo = (JSONObject) o;
                    contributorsElement.append("<contributor contributorType=\"" + type + "\"><contributorName>");
                    contributorsElement.append(jo.getString("givenName") + " "
                            + jo.getString("familyName"));
                    contributorsElement.append("</contributorName>");
                    contributorsElement.append("<nameIdentifier schemeURI=\"https://orcid.org/\" nameIdentifierScheme=\"ORCID\">" + jo.getString("@id") + "</nameIdentifier></contributor>");
                }
            }
        }
        return contributorsElement.toString();
    }

    public static String generateRelatedIdentifiers(JSONObject relatedIDs) {

        StringBuilder sb = new StringBuilder();
        boolean firstEntry = true;
        for (String key : relatedIDs.keySet()) {
            JSONArray entries = relatedIDs.getJSONArray(key);

            for(int i = 0 ; i < entries.length() ; i++) {
                Object val = entries.get(i);

//            Iterator<Object> iter = entries.iterator();
//            while (iter.hasNext()) {
//                Object val = iter.next();

                if (val instanceof String) {
                    String valString = ((String) val).trim();
                    logger.debug("Related type: " + valString);
                    if (!valString.contains(" ")) {
                        String valType = null;
                        if (valString.startsWith("doi") || valString.startsWith("https://doi.org/")) {
                            valType = "DOI";
                        } else if (valString.startsWith("urn:")) {
                            valType = "URN";
                        } else if (valString.startsWith("http")) {
                            valType = "URL";
                        }
                        if (valType != null) {
                            if (firstEntry) {
                                sb.append("<relatedIdentifiers>");
                                firstEntry=false;
                            }
                            sb.append("<relatedIdentifier relatedIdentifierType=\"" + valType + "\" relationType=\"" + key + "\">"
                                    + valString + "</relatedIdentifier>");
                        }
                    }
                }
            }
        }
        if (!firstEntry) {
            sb.append("</relatedIdentifiers>");
        }
        return sb.toString();
    }

    /*
     * public void generateFileIdentifiers(DvObject dvObject) {
     * 
     * if (dvObject.isInstanceofDataset()) { Dataset dataset = (Dataset) dvObject;
     * 
     * if (!dataset.getFiles().isEmpty() &&
     * !(dataset.getFiles().get(0).getIdentifier() == null)) {
     * 
     * datafileIdentifiers = new ArrayList<>(); for (DataFile dataFile :
     * dataset.getFiles()) { datafileIdentifiers.add(dataFile.getIdentifier()); int
     * x = xmlMetadata.indexOf("</relatedIdentifiers>") - 1; xmlMetadata =
     * xmlMetadata.replace("{relatedIdentifier}", dataFile.getIdentifier());
     * xmlMetadata = xmlMetadata.substring(0, x) +
     * "<relatedIdentifier relatedIdentifierType=\"hasPart\" " +
     * "relationType=\"doi\">${relatedIdentifier}</relatedIdentifier>" +
     * template.substring(x, template.length() - 1);
     * 
     * }
     * 
     * } else { xmlMetadata = xmlMetadata.
     * replace("<relatedIdentifier relatedIdentifierType=\"hasPart\" relationType=\"doi\">${relatedIdentifier}</relatedIdentifier>"
     * , ""); } } }
     */
    public static String getTemplate() {
        return template;
    }

    public static void setTemplate(String template) {
        DataCiteMetadataTemplate.template = template;
    }

    public void setDatasetIdentifier(String datasetIdentifier) {
        this.datasetIdentifier = datasetIdentifier;
    }

}

class Util {

    public static void close(InputStream in) {
        if (in != null) {
            try {
                in.close();
            } catch (IOException e) {
                throw new RuntimeException("Fail to close InputStream");
            }
        }
    }

    public static String readAndClose(InputStream inStream, String encoding) {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        byte[] buf = new byte[128];
        String data;
        try {
            int cnt;
            while ((cnt = inStream.read(buf)) >= 0) {
                outStream.write(buf, 0, cnt);
            }
            data = outStream.toString(encoding);
        } catch (IOException ioe) {
            throw new RuntimeException("IOException");
        } finally {
            close(inStream);
        }
        return data;
    }

    public static List<String> getListFromStr(String str) {
        return Arrays.asList(str.split("; "));
        // List<String> authors = new ArrayList();
        // int preIdx = 0;
        // for(int i=0;i<str.length();i++){
        // if(str.charAt(i)==';'){
        // authors.add(str.substring(preIdx,i).trim());
        // preIdx = i+1;
        // }
        // }
        // return authors;
    }

    public static String getStrFromList(List<String> authors) {
        StringBuilder str = new StringBuilder();
        for (String author : authors) {
            if (str.length() > 0) {
                str.append("; ");
            }
            str.append(author);
        }
        return str.toString();
    }

}
