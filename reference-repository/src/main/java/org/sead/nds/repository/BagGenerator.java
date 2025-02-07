/*
 * Copyright 2015 University of Michigan, 2016 The Trustees of Indiana University
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
 * @author myersjd@umich.edu
 * @author isuriara@indiana.edu
 */

package org.sead.nds.repository;

import java.io.*;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.archivers.zip.ParallelScatterZipCreator;
import org.apache.commons.compress.archivers.zip.ScatterZipOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntryRequest;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.WordUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.sead.nds.repository.util.FileUtils;
import org.sead.nds.repository.util.LinkRewriter;
import org.sead.nds.repository.util.LocalContentProvider;
import org.sead.nds.repository.util.NoOpLinkRewriter;
import org.sead.nds.repository.util.ValidationJob;

public class BagGenerator {

	private static final Logger log = LogManager.getLogger(BagGenerator.class);

	private ParallelScatterZipCreator scatterZipCreator = new ParallelScatterZipCreator(
			Executors.newFixedThreadPool(Repository.getNumThreads()));
	private ScatterZipOutputStream dirs = null;

	private JSONArray aggregates = null;
	private ArrayList<String> resourceIndex = null;
	private Boolean[] resourceUsed = null;
	private HashMap<String, String> pidMap = new LinkedHashMap<String, String>();
	private HashMap<String, String> sha1Map = new LinkedHashMap<String, String>();

	private String license = "No license information provided";
	private String purpose = "Production"; // Backward-compatibility - default
											// is for production

	private String hashtype = null;
	private boolean ignorehashes = false;

	private long dataCount = 0l;
	private long totalDataSize = 0l;

	private C3PRPubRequestFacade RO = null;
	private JSONObject pubRequest = null;
	private String bagID = null;

	private boolean usetemp = false;

	private LinkRewriter linkRewriter = new NoOpLinkRewriter();

	private LocalContentProvider lcProvider = null;

	public BagGenerator(C3PRPubRequestFacade ro) {
		RO = ro;
	}

	public void setIgnoreHashes(boolean val) {
		ignorehashes = val;
	}
	
	public void setLinkRewriter(LinkRewriter newRewriter) {
		linkRewriter = newRewriter;
	}

	/*
	 * Full workflow to generate new BagIt bag from ORE Map Url and to write the
	 * bag to the provided output stream (Ex: File OS, FTP OS etc.).
	 * 
	 * @return success true/false
	 */
	public boolean generateBag(OutputStream outputStream) throws Exception {
		log.info("Generating: Bag to the Future!");
		pubRequest = RO.getPublicationRequest();
		RO.sendStatus(C3PRPubRequestFacade.PENDING_STAGE, Repository.getID() + " is now processing this request");

		File tmp = File.createTempFile("sead-scatter-dirs", "tmp");
		dirs = ScatterZipOutputStream.fileBased(tmp);

		JSONObject oremap = RO.getOREMap();
		JSONObject aggregation = oremap.getJSONObject("describes");

		// Transfer statistics to oremap for preservation - note that the #
		// files, totalsize are checked after the zip is written
		// so any error will be recorded in the zip, but caught in the log.
		// Other elements are not curently checked.
		JSONObject aggStats = ((JSONObject) pubRequest.get("Aggregation Statistics"));
		aggregation.put("Aggregation Statistics", aggStats);

		if (((JSONObject) pubRequest.get(PubRequestFacade.PREFERENCES)).has("License")) {
			license = ((JSONObject) pubRequest.get(PubRequestFacade.PREFERENCES)).getString("License");

		}
		// Accept license preference and add it as the license on the
		// aggregation
		aggregation.put("License", license);

		if (((JSONObject) pubRequest.get(PubRequestFacade.PREFERENCES)).has("Purpose")) {
			purpose = ((JSONObject) pubRequest.get(PubRequestFacade.PREFERENCES)).getString("Purpose");

		}
		// Accept the purpose and add it to the map and aggregation (both are
		// for this purpose)
		aggregation.put("Purpose", purpose);
		oremap.put("Purpose", purpose);

		// check whether Access Rights set, if so, add it to aggregation
		if (((JSONObject) pubRequest.get(PubRequestFacade.PREFERENCES)).has("Access Rights")) {
			String accessRights = ((JSONObject) pubRequest.get(PubRequestFacade.PREFERENCES))
					.getString("Access Rights");
			aggregation.put("Access Rights", accessRights);
		}

		bagID = aggregation.getString("Identifier");
		String bagName = bagID;
		try {
			// Create valid filename from identifier and extend path with
			// two levels of hash-based subdirs to help distribute files
			bagName = getValidName(bagName);
		} catch (Exception e) {
			log.error("Couldn't create valid filename: " + e.getLocalizedMessage());
			return false;
		}
		// Create data dir in bag, also creates parent bagName dir
		String currentPath = bagName + "/data/";
		createDir(currentPath);

		aggregates = aggregation.getJSONArray("aggregates");

		if (aggregates != null) {
			// Add container and data entries
			// Setup global index of the aggregation and all aggregated
			// resources by Identifier
			resourceIndex = indexResources(bagID, aggregates);
			// Setup global list of succeed(true), fail(false), notused
			// (null) flags
			resourceUsed = new Boolean[aggregates.length() + 1];
			// Process current container (the aggregation itself) and its
			// children
			processContainer(aggregation, currentPath);
		}
		// Create maniifest files
		// pid-mapping.txt - a DataOne recommendation to connect ids and
		// in-bag path/names
		StringBuffer pidStringBuffer = new StringBuffer();
		boolean first = true;
		for (Entry<String, String> pidEntry : pidMap.entrySet()) {
			if (!first) {
				pidStringBuffer.append("\n");
			} else {
				first = false;
			}
			pidStringBuffer.append(pidEntry.getKey() + " " + pidEntry.getValue());
		}
		createFileFromString(bagName + "/pid-mapping.txt", pidStringBuffer.toString());
		// Hash manifest - a hash manifest is required
		// by Bagit spec
		StringBuffer sha1StringBuffer = new StringBuffer();
		first = true;
		for (Entry<String, String> sha1Entry : sha1Map.entrySet()) {
			if (!first) {
				sha1StringBuffer.append("\n");
			} else {
				first = false;
			}
			sha1StringBuffer.append(sha1Entry.getValue() + " " + sha1Entry.getKey());
		}
		if (!(hashtype == null)) {
			String manifestName = bagName + "/manifest-";
			if (hashtype.equals("SHA1 Hash")) {
				manifestName = manifestName + "sha1.txt";
			} else if (hashtype.equals("SHA512 Hash")) {
				manifestName = manifestName + "sha512.txt";
			} else {
				log.warn("Unsupported Hash type: " + hashtype);
			}
			createFileFromString(manifestName, sha1StringBuffer.toString());
		} else {
			log.warn("No Hash values sent - Bag File does not meet BagIT specification requirement");
		}
		// bagit.txt - Required by spec
		createFileFromString(bagName + "/bagit.txt", "BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8");

		if (oremap.getJSONObject("describes").has("Creator")) {
			aggregation.put("Creator",
					RO.expandPeople(RO.normalizeValues(oremap.getJSONObject("describes").get("Creator"))));
		}
		if (oremap.getJSONObject("describes").has("Contact")) {
			aggregation.put("Contact",
					RO.expandPeople(RO.normalizeValues(oremap.getJSONObject("describes").get("Contact"))));
		}

		// Generate DOI:
		oremap.getJSONObject("describes").put(PubRequestFacade.EXTERNAL_IDENTIFIER,
				Repository.createDOIForRO(bagID, RO));

		oremap.getJSONObject("describes").put("Publication Date",
				new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime()));

		Object context = oremap.get("@context");
		// FixMe - should test that these labels don't have a different
		// definition (currently we're just checking to see if they a
		// already defined)
		addIfNeeded(context, "License", "http://purl.org/dc/terms/license");
		addIfNeeded(context, "Purpose", "http://sead-data.net/vocab/publishing#Purpose");
		addIfNeeded(context, "Access Rights", "http://purl.org/dc/terms/accessRights");
		addIfNeeded(context, PubRequestFacade.EXTERNAL_IDENTIFIER, "http://purl.org/dc/terms/identifier");
		addIfNeeded(context, "Publication Date", "http://purl.org/dc/terms/issued");

		// Aggregation Statistics
		// For keys in Agg Stats:
		for (String key : ((Set<String>) aggStats.keySet())) {
			addIfNeeded(context, key, getURIForKey(pubRequest.get("@context"), key));
		}

		oremap.put("@id", linkRewriter.rewriteOREMapLink(oremap.getString("@id"), bagID));
		aggregation.put("@id", linkRewriter.rewriteAggregationLink(aggregation.getString("@id"), bagID));
		// Serialize oremap itself (pretty printed) - SEAD recommendation
		// (DataOne distributes metadata files within the bag
		// FixMe - add missing hash values if needed and update context
		// (read and cache files or read twice?)
		createFileFromString(bagName + "/oremap.jsonld.txt", oremap.toString(2));

		// Add a bag-info file
		createFileFromString(bagName + "/bag-info.txt", generateInfoFile(pubRequest, oremap));

		log.info("Creating bag: " + bagName);

		ZipArchiveOutputStream zipArchiveOutputStream = new ZipArchiveOutputStream(outputStream);

		// Add all the waiting contents - dirs created first, then data
		// files
		// are retrieved via URLs in parallel (defaults to one thread per
		// processor)
		// directly to the zip file
		log.debug("Starting write");
		writeTo(zipArchiveOutputStream);
		log.info("Zipfile Written");


		// Validate oremap - all entries are part of the collection
		for (int i = 0; i < resourceUsed.length; i++) {
			Boolean b = resourceUsed[i];
			if (b == null) {
				RO.sendStatus("Problem", pidMap.get(resourceIndex.get(i)) + " was not used");
			} else if (!b) {
				RO.sendStatus("Problem", pidMap.get(resourceIndex.get(i)) + " was not included successfully");
			} else {
				// Successfully included - now check for hash value and
				// generate if needed
				if (i > 0) { // Not root container
					if (!sha1Map.containsKey(pidMap.get(resourceIndex.get(i)))) {

						if (!RO.childIsContainer(i - 1))
							log.warn("Missing sha1 hash for: " + resourceIndex.get(i));
						// FixMe - actually generate it before adding the
						// oremap
						// to the zip
					}
				}
			}

		}
		return true;

	}

	public boolean generateBag(String bagName, boolean temp) {
		usetemp = temp;
		FileOutputStream bagFileOS = null;
		try {
			File origBagFile = getBagFile(bagName);
			File bagFile = origBagFile;
			if (usetemp) {
				bagFile = new File(bagFile.getAbsolutePath() + ".tmp");
				log.debug("Writing to: " + bagFile.getAbsolutePath());
			}
			// Create an output stream backed by the file
			bagFileOS = new FileOutputStream(bagFile);
			if (generateBag(bagFileOS)) {
				validateBagFile(bagFile);
				if (usetemp) {
					log.debug("Moving tmp zip");
					origBagFile.delete();
					bagFile.renameTo(origBagFile);
				}
				return true;
			} else {
				return false;
			}
		} catch (Exception e) {
			log.error("Bag Exception: ", e);
			e.printStackTrace();
			RO.sendStatus("Failure", "Processing failure during Bagit file creation");
			return false;
		} finally {
			IOUtils.closeQuietly(bagFileOS);
		}
	}

	public void validateBag(String bagId) {
		log.info("Validating Bag");
		ZipFile zf = null;
		InputStream is = null;
		try {
			zf = new ZipFile(getBagFile(bagId));
			ZipArchiveEntry entry = zf.getEntry(getValidName(bagId) + "/manifest-sha1.txt");
			if (entry != null) {
				log.info("SHA1 hashes used");
				hashtype = "SHA1 Hash";
				is = zf.getInputStream(entry);
				BufferedReader br = new BufferedReader(new InputStreamReader(is));
				String line = br.readLine();
				while (line != null) {
					log.debug("Hash entry: " + line);
					int breakIndex = line.indexOf(' ');
					String hash = line.substring(0, breakIndex);
					String path = line.substring(breakIndex + 1);
					log.debug("Adding: " + path + " with hash: " + hash);
					sha1Map.put(path, hash);
					line = br.readLine();
				}
				IOUtils.closeQuietly(is);

			} else {
				entry = zf.getEntry(getValidName(bagId) + "/manifest-sha512.txt");
				if (entry != null) {
					log.info("SHA512 hashes used");
					hashtype = "SHA512 Hash";
					is = zf.getInputStream(entry);
					BufferedReader br = new BufferedReader(new InputStreamReader(is));
					String line = br.readLine();
					while (line != null) {
						int breakIndex = line.indexOf(' ');
						String hash = line.substring(0, breakIndex);
						String path = line.substring(breakIndex + 1);
						sha1Map.put(path, hash);
						line = br.readLine();
					}
					IOUtils.closeQuietly(is);
				}
			}
			log.info("HashMap Map contains: " + sha1Map.size() + " etries");
			checkFiles(sha1Map, zf);
		} catch (IOException io) {
			log.error("Could not validate Hashes", io);
		} catch (Exception e) {
			log.error("Could not validate Hashes", e);
		}
		return;
	}

	public static File getBagFile(String bagID) throws Exception {
		String pathString = DigestUtils.sha1Hex(bagID);
		// Two level hash-based distribution o files
		String bagPath = Paths.get(Repository.getDataPath(), pathString.substring(0, 2), pathString.substring(2, 4))
				.toString();
		// Create the bag file on disk
		File parent = new File(bagPath);
		if (!parent.exists()) {
			parent.mkdirs();
		}
		// Create known-good filename
		String bagName = getValidName(bagID);
		File bagFile = new File(bagPath, bagName + ".zip");
		log.info("BagPath: " + bagFile.getAbsolutePath());
		// Create an output stream backed by the file
		return bagFile;
	}

	private void validateBagFile(File bagFile) throws IOException {
		// Run a confirmation test - should verify all files and hashes
		ZipFile zf = new ZipFile(bagFile);
		// Check files calculates the hashes and file sizes and reports on
		// whether hashes are correct
		// The file sizes are added to totalDataSize which is compared with the
		// stats sent in the request
		checkFiles(sha1Map, zf);

		log.info("Data Count: " + dataCount);
		log.info("Data Size: " + totalDataSize);
		// Check stats
		if (pubRequest.getJSONObject("Aggregation Statistics").getLong("Number of Datasets") != dataCount) {
			log.warn("Request contains incorrect data count: should be: " + dataCount);
		}
		// Total size is calced during checkFiles
		if (pubRequest.getJSONObject("Aggregation Statistics").getLong("Total Size") != totalDataSize) {
			log.warn("Request contains incorrect Total Size: should be: " + totalDataSize);
		}

		zf.close();
	}

	private void addIfNeeded(Object context, String key, String uri) {
		if (!isInContext(context, key)) {
			addToContext(context, key, uri);
		}
	}

	private boolean addToContext(Object context, String label, String predicate) {
		if (context instanceof JSONArray) {
			// Look for an object in the array to add to
			for (int i = 0; i < ((JSONArray) context).length(); i++) {
				if (addToContext(((JSONArray) context).get(i), label, predicate)) {
					return true;
				}
			}
		} else if (context instanceof JSONObject) {
			((JSONObject) context).put(label, predicate);
			return true;
		}
		return false;
	}

	private boolean isInContext(Object context, String label) {
		if (context instanceof JSONArray) {
			for (int i = 0; i < ((JSONArray) context).length(); i++) {
				if (isInContext(((JSONArray) context).get(i), label)) {
					return true;
				}
			}
		} else if (context instanceof JSONObject) {
			if (((JSONObject) context).has(label)) {
				return true;
			}
		}
		return false;
	}

	private String getURIForKey(Object context, String key) {
		String uri = null;
		if (context instanceof JSONArray) {
			for (int i = 0; i < ((JSONArray) context).length(); i++) {
				uri = getURIForKey(((JSONArray) context).get(i), key);
				if (uri != null) {
					return uri;
				}
			}
		} else if (context instanceof JSONObject) {
			if (((JSONObject) context).has(key)) {
				uri = ((JSONObject) context).getString(key);
			}
		}
		return uri;
	}

	public static String getValidName(String bagName) {
		// Create known-good filename
		return bagName.replaceAll("\\W", "_");
	}

	private void processContainer(JSONObject item, String currentPath) {
		JSONArray children = RO.getChildren(item);
		HashSet<String> titles = new HashSet<String>();
		currentPath = currentPath + item.getString("Title") + "/";
		int containerIndex = -1;
		try {
			createDir(currentPath);
			// Add containers to pid map and mark as 'used', but no sha1 hash
			// value
			containerIndex = getUnusedIndexOf(item.getString("Identifier"));
			resourceUsed[containerIndex] = true;
			pidMap.put(item.getString("Identifier"), currentPath);
		} catch (Exception e) {
			resourceUsed[containerIndex] = false;
			e.printStackTrace();
		}
		for (int i = 0; i < children.length(); i++) {

			// Find the ith child in the overall array of aggregated
			// resources
			String childId = children.getString(i);
			// Clowder Kludge
			if (childId.length() == 24) {
				childId = "urn:uuid:" + childId;
			}
			int index = getUnusedIndexOf(childId);
			if (resourceUsed[index] != null) {
				System.out.println("Warning: reusing resource " + index);
			}
			
			// Aggregation is at index 0, so need to shift by 1 for aggregates
			// entries
			JSONObject child = aggregates.getJSONObject(index - 1);
			if (RO.childIsContainer(index - 1)) {
				// create dir and process children
				//processContainer will mark this item as used
				processContainer(child, currentPath);
			} else {
				resourceUsed[index] = true;
				// add item
				String dataUrl = (String) child.get("similarTo");
				String title = (String) child.get("Label"); //Labels were filenames and should make usable paths, whereas titles may have non-standard chars
				if (titles.contains(title)) {
					log.warn("**** Multiple items with the same title in: " + currentPath);
					log.warn("**** Will cause failure in hash and size validation.");
				} else {
					titles.add(title);
				}
				String childPath = currentPath + title;

				String childHash = null;
				if (child.has("SHA1 Hash")) {
					if (hashtype != null && !hashtype.equals("SHA1 Hash")) {
						log.warn("Multiple hash values in use - not supported");
					}
					hashtype = "SHA1 Hash";
					childHash = child.getString("SHA1 Hash").replaceAll("^\"|\"$", "");
					if (sha1Map.containsValue(childHash)) {
						// Something else has this hash
						log.warn("Duplicate/Collision: " + child.getString("Identifier") + " has SHA1 Hash: "
								+ childHash);
					}
					sha1Map.put(childPath, childHash);
				}
				if (child.has("SHA512 Hash")) {
					if (hashtype != null && !hashtype.equals("SHA512 Hash")) {
						log.warn("Multiple has values in use - not supported");
					}
					hashtype = "SHA512 Hash";
					childHash = child.getString("SHA512 Hash").replaceAll("^\"|\"$", "");
					if (sha1Map.containsValue(childHash)) {
						// Something else has this hash
						log.warn("Duplicate/Collision: " + child.getString("Identifier") + " has SHA512 Hash: "
								+ childHash);
					}
					sha1Map.put(childPath, childHash);
				}
				if((hashtype==null) | ignorehashes) {
					//Pick sha512 when ignoring hashes or none exist
					hashtype = "SHA512 Hash";
				}
				try {
					boolean success = false;
					if ((lcProvider != null) && (childHash != null)) {
						log.debug("Requesting local content for " + childPath);
						success = createFileFromLocalSource(childPath, hashtype, childHash);
					}
					if (!success) {
						if((childHash==null)| childHash.length()==0 | ignorehashes) {
							//Generate missing hashInputStream inputStream = null;
							InputStream inputStream = null;
							try {
								inputStream = RO.getInputStreamSupplier(dataUrl).get();
								
								if (hashtype != null) {
									if (hashtype.equals("SHA1 Hash")) {
										childHash = DigestUtils.sha1Hex(inputStream);
									} else if (hashtype.equals("SHA512 Hash")) {
										childHash = DigestUtils.sha512Hex(inputStream);
									}
								}
								
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} finally {
								IOUtils.closeQuietly(inputStream);
							}
							if(childHash!=null) {
								child.put(hashtype, childHash);
								sha1Map.put(childPath, childHash);
							} else {
								log.warn("Unable to calculate a " + hashtype + " for " + dataUrl);
							}
						}
						log.debug("Requesting: " + childPath + " from " + dataUrl);
						createFileFromURL(childPath, dataUrl);
					}
					dataCount++;
					if (dataCount % 1000 == 0) {
						log.info("Retrieval in progress: " + dataCount + " files retrieved");
					}
				} catch (Exception e) {
					resourceUsed[index] = false;
					e.printStackTrace();
				}

				// Check for nulls!
				pidMap.put(child.getString("Identifier"), childPath);
				child.put("similarTo", linkRewriter.rewriteDataLink(dataUrl, child.getString("@id"), bagID, childPath));

			}
		}
	}

	private int getUnusedIndexOf(String childId) {
		int index = resourceIndex.indexOf(childId);
		if (resourceUsed[index] != null) {
			System.out.println("Warning: reusing resource " + index);
		}

		while (resourceUsed[index] != null) {
			int offset = index;
			index = offset + 1 + resourceIndex.subList(offset + 1, resourceIndex.size()).indexOf(childId);
		}
		System.out.println("Using index: " + index);
		if (index == -1) {
			log.error("Reused ID: " + childId + " not found enough times in resource list");
		}
		return index;
	}

	private ArrayList<String> indexResources(String aggId, JSONArray aggregates) {

		ArrayList<String> l = new ArrayList<String>(aggregates.length() + 1);
		l.add(aggId);
		for (int i = 0; i < aggregates.length(); i++) {
			log.debug("Indexing : " + i + " " + aggregates.getJSONObject(i).getString("Identifier"));
			l.add(aggregates.getJSONObject(i).getString("Identifier"));
		}
		log.info("Index created for " + aggregates.length() + " entries");
		return l;
	}

	private void createDir(final String name) throws IOException, ExecutionException, InterruptedException {

		ZipArchiveEntry archiveEntry = new ZipArchiveEntry(name);
		archiveEntry.setMethod(ZipEntry.DEFLATED);
		InputStreamSupplier supp = new InputStreamSupplier() {
			public InputStream get() {
				return new ByteArrayInputStream(("").getBytes());
			}
		};

		addEntry(archiveEntry, supp);
	}

	private void createFileFromString(final String name, final String content)
			throws IOException, ExecutionException, InterruptedException {

		ZipArchiveEntry archiveEntry = new ZipArchiveEntry(name);
		archiveEntry.setMethod(ZipEntry.DEFLATED);
		InputStreamSupplier supp = new InputStreamSupplier() {
			public InputStream get() {
				try {
					return new ByteArrayInputStream(content.getBytes("UTF-8"));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null;
			}
		};

		addEntry(archiveEntry, supp);
	}

	private void createFileFromURL(final String name, final String uri)
			throws IOException, ExecutionException, InterruptedException {

		ZipArchiveEntry archiveEntry = new ZipArchiveEntry(name);
		archiveEntry.setMethod(ZipEntry.DEFLATED);
		InputStreamSupplier supp = RO.getInputStreamSupplier(uri);
		addEntry(archiveEntry, supp);
	}

	private boolean createFileFromLocalSource(String name, String hashtype, String childHash) throws IOException {
		InputStreamSupplier supp = lcProvider.getSupplierFor(hashtype, childHash);
		if (supp != null) {
			ZipArchiveEntry archiveEntry = new ZipArchiveEntry(name);
			archiveEntry.setMethod(ZipEntry.DEFLATED);
			addEntry(archiveEntry, supp);
			return true;
		}
		return false;
	}

	private void checkFiles(HashMap<String, String> shaMap, ZipFile zf) {
		ExecutorService executor = Executors.newFixedThreadPool(Repository.getNumThreads());
		ValidationJob.setZipFile(zf);
		ValidationJob.setBagGenerator(this);
		log.info("Validating hashes for zipped data files");
		int i = 0;
		for (Entry<String, String> entry : shaMap.entrySet()) {

			ValidationJob vj = new ValidationJob(entry.getValue(), entry.getKey());
			executor.execute(vj);
			i++;
			if (i % 1000 == 0) {
				log.info("Queuing Hash Validations: " + i);
			}
		}
		log.info("All Hash Validations Queued: " + i);

		executor.shutdown();
		try {
			while (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
				log.debug("Awaiting completion of hash calculations.");
			}
		} catch (InterruptedException e) {
			log.error("Hash Calculations interrupted", e);
		}
		log.info("Hash Validations Completed");
	}

	public void addEntry(ZipArchiveEntry zipArchiveEntry, InputStreamSupplier streamSupplier) throws IOException {
		if (zipArchiveEntry.isDirectory() && !zipArchiveEntry.isUnixSymlink())
			dirs.addArchiveEntry(ZipArchiveEntryRequest.createZipArchiveEntryRequest(zipArchiveEntry, streamSupplier));
		else
			scatterZipCreator.addArchiveEntry(zipArchiveEntry, streamSupplier);
	}

	public void writeTo(ZipArchiveOutputStream zipArchiveOutputStream)
			throws IOException, ExecutionException, InterruptedException {
		log.debug("Writing dirs");
		dirs.writeTo(zipArchiveOutputStream);
		log.debug("Dirs written");
		scatterZipCreator.writeTo(zipArchiveOutputStream);
		log.debug("Files written");
		// Finish
		dirs.close();
		zipArchiveOutputStream.close();
		log.debug("Closed");
	}

	static final String CRLF = "\r\n";

	private String generateInfoFile(JSONObject request, JSONObject map) {
		log.debug("Generating info file");
		StringBuffer info = new StringBuffer();

		// SEAD and Rights Holder?
		if (map.getJSONObject("describes").has("Primary Source")) {
			info.append("Source-Organization: ");
			info.append(map.getJSONObject("describes").getString("Primary Source"));
			info.append(CRLF);
		}
		JSONArray contactsArray = new JSONArray();
		if (map.getJSONObject("describes").has("Contact")) {

			log.debug(map.getJSONObject("describes").get("Contact").toString());
			contactsArray = map.getJSONObject("describes").getJSONArray("Contact");
		} else {
			if (request.has("Rights Holder")) {
				contactsArray = RO.expandPeople(RO.normalizeValues(request.get("Rights Holder")));
			}
		}
		for (int i = 0; i < contactsArray.length(); i++) {
			info.append("Contact-Name: ");
			Object person = contactsArray.get(i);
			if (person instanceof String) {
				info.append((String) person);
				info.append(CRLF);

			} else {
				info.append(
						((JSONObject) person).getString("givenName") + ((JSONObject) person).getString("familyName"));
				info.append(CRLF);
				if (((JSONObject) person).has("email")) {
					info.append("Contact-Email: ");
					info.append(((JSONObject) person).getString("email"));
					info.append(CRLF);
				}
			}
		}

		info.append("Source-Organization: ");
		info.append("SEAD (http://sead-data.net/)");
		info.append(CRLF);

		info.append("Organization-Address: ");
		info.append("University of Michigan, Ann Arbor, MI 48106");
		info.append(CRLF);

		info.append("Contact-Email: ");
		info.append("SEADdatanet@umich.edu");
		info.append(CRLF);

		info.append("External-Description: ");

		info.append(WordUtils.wrap(getSingleValue(map.getJSONObject("describes"), "Abstract"), 78, CRLF + " ", true));

		info.append(CRLF);

		info.append("Bagging-Date: ");
		info.append((new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime())));
		info.append(CRLF);

		info.append("External-Identifier: ");
		info.append(map.getJSONObject("describes").getString(PubRequestFacade.EXTERNAL_IDENTIFIER));
		info.append(CRLF);

		info.append("Bag-Size: ");
		info.append(FileUtils
				.byteCountToDisplaySize(request.getJSONObject("Aggregation Statistics").getLong("Total Size")));
		info.append(CRLF);

		info.append("Payload-Oxum: ");
		info.append(request.getJSONObject("Aggregation Statistics").getLong("Total Size"));
		info.append(".");
		info.append(request.getJSONObject("Aggregation Statistics").getLong("Number of Datasets"));
		info.append(CRLF);

		info.append("Internal-Sender-Identifier: ");
		info.append(map.getJSONObject("describes").getString("Identifier"));
		info.append(CRLF);

		return info.toString();

	}

	/**
	 * Kludge - handle when a single string is sent as an array of 1 string and,
	 * for cases where multiple values are sent when only one is expected,
	 * create a concatenated string so that information is not lost.
	 * 
	 * @param parent
	 *			- the root json object
	 * @param key
	 *			- the key to find a value(s) for
	 * @return - a single string
	 */
	String getSingleValue(JSONObject parent, String key) {
		String val = "";
		if (parent.get(key) instanceof String) {
			val = parent.get(key).toString();
		} else if (parent.get(key) instanceof ArrayList) {
			val = StringUtils.join(((ArrayList) parent.get(key)).toArray(), ",");
			log.warn("Multiple values found for: " + key + ": " + val);
		}
		return val;
	}

	public void setLocalContentProvider(LocalContentProvider lcProvider) {
		this.lcProvider = lcProvider;
	}

	// Used in validation

	public void incrementTotalDataSize(long inc) {
		totalDataSize += inc;
	}

	public PubRequestFacade getRO() {
		return RO;
	}

	public String getHashtype() {
		return hashtype;
	}
}
