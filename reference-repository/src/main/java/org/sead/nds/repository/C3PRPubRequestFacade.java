/*
 * Copyright 2015 University of Michigan
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
 */

package org.sead.nds.repository;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import javax.net.ssl.SSLContext;
import javax.ws.rs.core.MediaType;

import org.apache.commons.compress.parallel.InputStreamSupplier;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class C3PRPubRequestFacade extends PubRequestFacade {

	private static final Logger log = LogManager.getLogger(C3PRPubRequestFacade.class);

	private int timeout = 1200;
	private RequestConfig config = RequestConfig.custom().setConnectTimeout(timeout * 1000)
			.setConnectionRequestTimeout(timeout * 1000).setSocketTimeout(timeout * 1000).build();

	private static HttpClientContext localContext = HttpClientContext.create();

	private PoolingHttpClientConnectionManager cm = null;
	protected CloseableHttpClient client;

	Properties props = null;
	String RO_ID = null;
	String bearerToken = null;

	protected JSONObject request = null;
	protected JSONObject repository = null;
	protected JSONObject oremap = null;

	public C3PRPubRequestFacade(String RO_ID, Properties props)
			throws NoSuchAlgorithmException, KeyManagementException {
		this.RO_ID = RO_ID;
		this.props = props;
		if (props.getProperty("c3pr.address").startsWith("https")) {
			SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
			sslContext.init(null, null, null);

			SSLConnectionSocketFactory sslConnectionFactory = new SSLConnectionSocketFactory(sslContext,
					NoopHostnameVerifier.INSTANCE);

			Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create()
					.register("https", sslConnectionFactory).build();
			cm = new PoolingHttpClientConnectionManager(registry);
		} else {
			cm = new PoolingHttpClientConnectionManager();
		}
		cm.setDefaultMaxPerRoute(Repository.getNumThreads());
		cm.setMaxTotal(Repository.getNumThreads() > 20 ? Repository.getNumThreads() : 20);

		client = HttpClients.custom().setConnectionManager(cm).setDefaultRequestConfig(config).build();

	}

	protected String proxyIfNeeded(String urlString) {

		if (props.containsKey("proxyserver")) {
			return props.getProperty("c3pr.address") + urlString.substring(urlString.indexOf("api"));
		} else {
			return urlString;
		}
	}

	public JSONObject getPublicationRequest() {
		if (request == null) {
			String c3prServer = props.getProperty("c3pr.address");
			HttpGet getPubRequest;
			CloseableHttpResponse response = null;
			try {
				log.debug("Retrieving: " + c3prServer + "api/researchobjects/" + URLEncoder.encode(RO_ID, "UTF-8"));
				getPubRequest = new HttpGet(c3prServer + "api/researchobjects/" + URLEncoder.encode(RO_ID, "UTF-8"));

				getPubRequest.addHeader("accept", "application/json");

				response = client.execute(getPubRequest, getLocalContext());

				if (response.getStatusLine().getStatusCode() == 200) {
					String mapString = EntityUtils.toString(response.getEntity());
					log.trace(mapString);
					request = new JSONObject(mapString);

				}
			} catch (UnsupportedEncodingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				log.error("Unable to retrieve pub request document", e);
				e.printStackTrace();
			} finally {
				if(response!=null) {
					EntityUtils.consumeQuietly(response.getEntity());
				}
			}

			// Set the bearerToekn for any calls to the originating space
			// related to
			// this RO
			if (request.has("Bearer Token")) {
				bearerToken = request.getString("Bearer Token");
			} else if (props.containsKey("bearertoken.default")) {
				bearerToken = props.getProperty("bearertoken.default");
			}
		}
		return request;
	}

	public JSONObject getRepositoryProfile() {
		if (repository == null) {
			// Make sure we have retrieved the request so we can look for the
			// repository being requested
			if (request == null) {
				request = getPublicationRequest();
			}
			if (request.has("Repository")) {
				String repo = request.getString("Repository");

				String c3prServer = props.getProperty("c3pr.address");
				HttpGet getRepoRequest;
				CloseableHttpResponse response = null;
				try {
					log.debug("Retrieving: " + c3prServer + "api/repositories/" + URLEncoder.encode(repo, "UTF-8"));
					getRepoRequest = new HttpGet(c3prServer + "api/repositories/" + URLEncoder.encode(repo, "UTF-8"));

					getRepoRequest.addHeader("accept", "application/json");

					response = client.execute(getRepoRequest, getLocalContext());

					if (response.getStatusLine().getStatusCode() == 200) {
						String mapString = EntityUtils.toString(response.getEntity());
						log.trace(mapString);
						repository = new JSONObject(mapString);

					}
				} catch (UnsupportedEncodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (ClientProtocolException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					log.error("Unable to retrieve repository document", e);
					e.printStackTrace();
				} finally {
					if(response!=null) {
						EntityUtils.consumeQuietly(response.getEntity());
					}
				}
			}
		}
		return repository;
	}

	private URI getOREMapURI() {
		try {
			return new URI(proxyIfNeeded(request.getJSONObject("Aggregation").getString("@id")));
		} catch (JSONException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public JSONObject getOREMap() {
		if (oremap == null) {

			log.debug("Retreiving: " + getOREMapURI().toString());

			HttpGet getMap = new HttpGet(getOREMapURI());
			getMap.addHeader("accept", MediaType.APPLICATION_JSON);
			CloseableHttpResponse response = null;
			try {
				response = client.execute(getMap, getLocalContext());

				if (response.getStatusLine().getStatusCode() == 200) {
					String mapString = EntityUtils.toString(response.getEntity());
					log.trace("OREMAP: " + mapString);
					oremap = new JSONObject(mapString);
				}
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				log.error("Unable to retrieve OREMap", e);
				e.printStackTrace();
			} finally {
				if(response!=null) {
					EntityUtils.consumeQuietly(response.getEntity());
				}
			}
		}
		return oremap;
	}

	private HttpGet createNewGetRequest(URI url, String returnType) {

		HttpGet request = null;

		if (bearerToken != null) {
			// Clowder Kluge - don't add key once Auth header is accepted

			try {
				//Add a key if the url doesn't have one
				if (!url.toURL().toString().contains("?key=")) {
					request = new HttpGet(new URI(url.toURL().toString() + "?key=" + bearerToken));
				} else {
					request = new HttpGet(url);
				}
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			request.addHeader("Authorization", "Bearer " + bearerToken);
		} else {
			request = new HttpGet(url);
		}
		if (returnType != null) {
			request.addHeader("accept", returnType);
		}
		return request;
	}

	InputStreamSupplier getInputStreamSupplier(final String uri) {

		return new InputStreamSupplier() {
			public InputStream get() {
				int tries = 0;
				CloseableHttpResponse response = null;
				while (tries < 5) {
					try {
						HttpGet getMap = createNewGetRequest(new URI(uri), null);
						log.trace("Retrieving " + tries + ": " + uri);
						
						response = client.execute(getMap, getLocalContext());
						if (response.getStatusLine().getStatusCode() == 200) {
							log.trace("Retrieved: " + uri);
							return response.getEntity().getContent();
						} else {
							EntityUtils.consumeQuietly(response.getEntity());
						}
						log.debug("Status: " + response.getStatusLine().getStatusCode());
						tries++;

					} catch (ClientProtocolException e) {
						tries += 5;
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// Retry if this is a potentially temporary error such
						// as a timeout
						tries++;
						log.warn("Attempt# " + tries + " : Unable to retrieve file: " + uri, e);
						if (tries == 5) {
							log.error("Final attempt failed for " + uri);
						}
						e.printStackTrace();
					} catch (URISyntaxException e) {
						tries += 5;
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				log.error("Could not read: " + uri);
				return null;
			}
		};
	}

	JSONArray expandPeople(String[] people) {

		JSONArray peopleArray = new JSONArray();
		if ((people != null) && (people.length != 0)) {
			String c3prServer = props.getProperty("c3pr.address");
			for (int i = 0; i < people.length; i++) {
				log.debug("Expanding: " + people[i]);
				try {
					log.debug(URLEncoder.encode(people[i], "UTF-8"));
				} catch (UnsupportedEncodingException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				HttpGet getPerson;
				CloseableHttpResponse response = null;
				try {
					getPerson = new HttpGet(c3prServer + "api/people/" + URLEncoder.encode(people[i], "UTF-8"));

					getPerson.addHeader("accept", "application/json");
					log.trace("getPerson created" + getPerson.getURI());
					response = client.execute(getPerson, getLocalContext());

					if (response.getStatusLine().getStatusCode() == 200) {
						String mapString = EntityUtils.toString(response.getEntity());
						log.trace("Expanded: " + mapString);
						peopleArray.put(new JSONObject(mapString));

					} else {
						log.trace("Adding unexpanded person: " + people[i]);
						peopleArray.put(people[i]);
					}
					// Required to avoid some calls hanging in execute() even
					// though we do not reuse the HttpGet object
					getPerson.reset();

				} catch (UnsupportedEncodingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (ClientProtocolException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalArgumentException ia) {
					ia.printStackTrace();
				} finally {
					if(response!=null) {
						EntityUtils.consumeQuietly(response.getEntity());
					}
				}
			}
			log.debug("Expansion complete");
		}
		return peopleArray;
	}

	public void sendStatus(String stage, String message) {

		String c3prServer = props.getProperty("c3pr.address");
		CloseableHttpResponse response=null;
		try {
			String statusUrl = c3prServer + "api/researchobjects/" + URLEncoder.encode(RO_ID, "UTF-8") + "/status";

			log.debug("Posting status to: " + statusUrl);
			HttpPost postStatus = new HttpPost(statusUrl);

			postStatus.addHeader("accept", MediaType.APPLICATION_JSON);
			String statusString = "{\"reporter\":\"" + Repository.getID() + "\", \"stage\":\"" + stage
					+ "\", \"message\":\"" + message + "\"}";
			StringEntity status = new StringEntity(statusString);
			log.trace("Status: " + statusString);
			postStatus.addHeader("content-type", MediaType.APPLICATION_JSON);
			postStatus.setEntity(status);

			response = client.execute(postStatus, getLocalContext());

			if (response.getStatusLine().getStatusCode() == 200) {
				log.debug("Status Successfully posted");
			} else {
				log.warn("Failed to post status, response code: " + response.getStatusLine().getStatusCode());
			}

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			log.error("Error posting status.", e);

			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			log.error("Error posting status.", e);

			e.printStackTrace();
		} catch (IOException e) {
			log.error("Error posting status.", e);
			e.printStackTrace();
		} catch (Exception e) {
			log.error("Odd Error posting status.", e);
			e.printStackTrace();

		} finally {
			  // Must consume entity to allow connection to be released
			// If this line is not here, the third try to send status will
			// result in a
			// org.apache.http.conn.ConnectionPoolTimeoutException: Timeout
			// waiting for connection from pool
			// (or a blocked call/hung program if timeouts weren't set
			if(response!=null) {
				EntityUtils.consumeQuietly(response.getEntity());
			}
		}

		if (echoToConsole) {
			System.out.println("*********************Status Message******************************");
			System.out.println("Reporter: " + Repository.getID() + ", Stage: " + stage);
			System.out.println("Message Text: " + message);
			System.out.println("*****************************************************************");
		}
	}

	public HttpClientContext getLocalContext() {
		return localContext;
	}

	public void setLocalContext(HttpClientContext lc) {
		localContext = lc;
	}

}
