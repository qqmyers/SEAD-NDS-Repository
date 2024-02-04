/*
 *
 * Copyright 2017 University of Michigan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * @author myersjd@umich.edu
 * 
 */

package org.sead.repositories.reference.tools;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sead.nds.repository.Repository;
import org.sead.repositories.reference.RefRepository;

public class MovePublication {

	private static final Logger log = LogManager.getLogger(MovePublication.class);

	private static String similarTo = "similarTo";
	private static String ID = "@id";
    private static String externalID = "External Identifier";

	private static Map<String, String> pidMap = null;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: MovePublication <id> <new base URL>");
			System.out
					.println("<id> should correspond to the name of a local zip file");
			System.exit(0);

		}

		String id = args[0];
		String base = args[1];
		log.info("Moving Publication: " + id + " to server " + base);
		System.out.println("Moving Publication: " + id + " to server " + base);

		JSONObject oremap = null;

		String bagNameRoot = RefRepository.getBagNameRoot(id);
		
		Properties inputProps = new Properties();
		inputProps.put("repo.datapath", "./");
		inputProps.put("repo.ID", "Bob"); // Needed but not used.
		Repository.init(inputProps);
		String inputPath = RefRepository.getDataPathTo(id);
		System.out.println(inputPath + " / " + bagNameRoot);
		File result = new File(inputPath, bagNameRoot + ".zip");
		ZipFile zf = null;
		try {
			zf = new ZipFile(result);
			InputStream oreIS = null;
			
			ZipEntry archiveEntry1 = zf.getEntry(bagNameRoot
					+ "/oremap.jsonld.txt");

			if (archiveEntry1 != null) {
				oreIS = new BufferedInputStream(
						zf.getInputStream(archiveEntry1));
				oremap = new JSONObject(IOUtils.toString(oreIS, "UTF-8"));
			}
			InputStream pidIS = null;
			ZipEntry archiveEntry2 = zf.getEntry(bagNameRoot
					+ "/pid-mapping.txt");
			if (archiveEntry2 != null) {
				pidIS = new BufferedInputStream(
						zf.getInputStream(archiveEntry2));
				pidMap = readPidMap(pidIS);
			}

			IOUtils.closeQuietly(oreIS);
			IOUtils.closeQuietly(pidIS);

		} catch (IOException e) {
			log.warn("Can't find entries: ", e);
		} finally {
			IOUtils.closeQuietly(zf);
		}

		/*
		 * Now, scan oremap for similarTo entries and map them to new URLs For
		 * collections - leave as is For datasets (type
		 * http://cet.ncsa.uiuc.edu/2007/Dataset or
		 * http://cet.ncsa.uiuc.edu/2015/File ver):
		 */

        oremap.put(ID, newLocation(oremap.getString(ID), base));
        
        JSONObject agg = oremap.getJSONObject("describes");
        //Update old DOI strings
        agg.put(externalID, agg.getString(externalID).replace("dx.doi", "doi"));
        
        agg.put(ID, newLocation(agg.getString(ID), base));
        JSONArray aggregates = agg.getJSONArray(
                "aggregates");
        for (int i = 0; i < aggregates.length(); i++) {
			JSONObject resource = aggregates.getJSONObject(i);
			if (isData(resource)) {
				resource.put(similarTo, newLocation(resource.getString(similarTo), base));
			}
		}

		//Freshen zip
		try {
			FileSystem zfs = FileSystems.newFileSystem(result.toPath());
			Files.copy(new ByteArrayInputStream(oremap.toString(2).getBytes(StandardCharsets.UTF_8)), zfs.getPath(bagNameRoot + "/oremap.jsonld.txt"), StandardCopyOption.REPLACE_EXISTING); 
			zfs.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	private static String newLocation(String location, String base) {
		/*
		 * SDA-style: URLs contain /resteasy/researchobjects/<RO
		 * ID>/files/<URLencoded live ID>?pubtoken=<key> Map to
		 * base/api/researchobjecs/<RO ID>/<path starting at data/...>
		 * 
		 * RefRepoStyle - just replace base before the /api/researchobjects
		 * ...// TODO Auto-generated method stub
		 */
		String newLocation = location; // Default - leave as is.
		if (location.contains("/resteasy/researchobjects/")) {
			// SDA style
			String ro_id = location.substring(location
					.indexOf("/resteasy/researchobjects/")
					+ "/resteasy/researchobjects/".length());
			String id = ro_id;
			ro_id = ro_id.substring(0, ro_id.indexOf("/"));
			id = id.substring(id.indexOf("/files/") + "/files/".length());
			id = id.substring(0, id.indexOf("?pubtoken"));
			try {
				id = URLDecoder.decode(id, "UTF-8");
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(ro_id);
			System.out.println(id);
			String path = pidMap.get(id);
	         //Some paths don't escape the / chars in the internal path as required for the ref repository
			int offset=path.indexOf("/data/");
			if(offset>=0) {
			path = path.substring(offset, offset+6) + path.substring(offset+6).replace("/",  "%2F");
			}
			newLocation = (base + "/api/researchobjects/" + ro_id + path);
		} else if (location.contains("/api/researchobjects/")) {
			// Ref style
		    String newPath = location.substring(location.indexOf("/api/researchobjects/"));
		    //Some paths don't escape the / chars in the internal path as required for the ref repository
		    int offset = newPath.indexOf("/data/");
		    if(offset >=0) {
		      newPath = newPath.substring(0,offset+6) + newPath.substring(offset+6).replace("/", "%2F");
		    }
			newLocation = base + newPath;
		}
		if(newLocation.endsWith("oremap")) {
		    newLocation = newLocation.substring(0,newLocation.length()-6) + "meta/oremap.jsonld.txt";
		}
		if(newLocation.endsWith("oremap#aggregation")) {
            newLocation = newLocation.substring(0,newLocation.length()-18) + "meta/oremap.jsonld.txt#aggregation";
        }
		return newLocation;
	}

	static String seadDataType = "http://cet.ncsa.uiuc.edu/2007/Dataset";
	static String sead2DataType = "http://cet.ncsa.uiuc.edu/2015/File";

	private static boolean isData(JSONObject resource) {
		boolean isData = false;
		Object type = resource.get("@type");
		if (type != null) {
			if (type instanceof JSONArray) {
				for (int j = 0; j < ((JSONArray) type).length(); j++) {
					String theType = ((JSONArray) type).getString(j);
					if (theType.equals(sead2DataType)
							|| theType.equals(seadDataType)) {
						isData = true;
					}
				}
			}
		}
		return isData;
	}

	private static Map<String, String> readPidMap(InputStream is) {
		Map<String, String> pidMap = new HashMap<String, String>();
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line;
		try {
			line = br.readLine();
			while (line != null) {
				int firstSpace = line.indexOf(' ');
				String id = line.substring(0, firstSpace);
				if (id.contains("/")) {
					id = id.substring(0, id.lastIndexOf("/"));
				}
				String path = line.substring(firstSpace + 1);

				pidMap.put(id, path);

				line = br.readLine();
			}
		} catch (IOException e) {
			log.warn("Error reading ID to path info from pid-mapping.txt file: "
					+ e.getLocalizedMessage());
			e.printStackTrace();
		}
		return pidMap;
	}

}
