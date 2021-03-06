/*
 * Copyright 2016 Attribyte, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.attribyte.relay;

import com.attribyte.client.ClientProtos;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.attribyte.api.InvalidURIException;
import org.attribyte.api.Logger;
import org.attribyte.api.http.GetRequestBuilder;
import org.attribyte.api.http.Request;
import org.attribyte.api.http.Response;
import org.attribyte.api.http.impl.BasicAuthScheme;
import org.attribyte.essem.metrics.Timer;
import org.attribyte.util.URIEncoder;
import org.attribyte.api.http.AsyncClient;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.attribyte.relay.util.FileUtil.getExtension;

/**
 * A client for Attribyte's "duster" on-demand image resize service.
 */
public class DusterClient implements MetricSet {

   /**
    * Creates the client.
    * <ul>
    *    <li>imageDimensionURL - The duster service URL</li>
    *    <li>imageDimensionUsername - The duster service auth username</li>
    *    <li>imageDimensionPassword - The duster service auth password</li>
    *    <li>minWidth - The minimum width</li>
    *    <li>minHeight - The minimum height</li>
    *    <li>minAspect - The minimum aspect ratio</li>
    *    <li>maxAspect - The maximum aspect ratio</li>
    *    <li>pregenAllTransforms - Should all available transforms be pre-generated, async?</li>
    * </ul>
    * @param props The configuration properties.
    * @param httpClient The HTTP client.
    * @param logger A logger.
    * @throws Exception on invalid properties or initialization problem.
    */
   public DusterClient(final Properties props,
                       final AsyncClient httpClient,
                       final Logger logger) throws Exception {

      this.imageDimensionURL = props.getProperty("imageDimensionURL");
      if(this.imageDimensionURL == null) {
         throw new Exception("The 'imageDimensionURL' must be specified");
      }

      String username = Strings.nullToEmpty(props.getProperty("imageDimensionUsername")).trim();
      if(username.isEmpty()) {
         throw new Exception("The 'imageDimensionUsername' must be specified");
      }

      String password = Strings.nullToEmpty(props.getProperty("imageDimensionPassword")).trim();
      if(password.isEmpty()) {
         throw new Exception("The 'imageDimensionPassword' must be specified");
      }

      this.auth = BasicAuthScheme.buildAuthHeaderValue(username, password);

      this.minWidth = Integer.parseInt(props.getProperty("minWidth", "50"));
      this.minHeight = Integer.parseInt(props.getProperty("minHeight", "50"));
      this.minAspect = Double.parseDouble(props.getProperty("minAspect", "0.25"));
      this.maxAspect = Double.parseDouble(props.getProperty("maxAspect", "5"));
      this.httpClient = httpClient;
      this.availableTransforms = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(Strings.nullToEmpty(props.getProperty("transforms"))));
      if(props.getProperty("pregenerateAll", "false").trim().equalsIgnoreCase("true")) {
         pregenerateTransformQuery = "transformAll=true";
      } else if(this.availableTransforms.size() == 0) {
         pregenerateTransformQuery = "transformAll=false";
      } else {
         pregenerateTransformQuery = Joiner.on('&').join(this.availableTransforms.stream().map(transform -> "transform=" + transform).collect(Collectors.toList()));
      }
      this.transformPath = props.getProperty("transformPath", "");

      this.logger = logger;
   }

   /**
    * Enables duster images for all images found in the entry.
    * @param entry The entry.
    */
   public void enableImages(final ClientProtos.WireMessage.Entry.Builder entry) {
      if(entry.getImagesCount() > 0) {
         List<ClientProtos.WireMessage.Image> replaceImages = Lists.newArrayListWithCapacity(entry.getImagesCount());
         List<ClientProtos.WireMessage.Image> images = entry.getImagesList();
         for(ClientProtos.WireMessage.Image image : images) {
            try {
               ImageMeta meta = enableImage(image);
               String ext = getExtension(image.getOriginalSrc()).toLowerCase();
               if(meta != null && isExtensionAllowed(ext)) {
                  ClientProtos.WireMessage.Image.Builder builder = ClientProtos.WireMessage.Image.newBuilder(image);
                  builder.setUrl(transformPath + meta.sourceHash + ext);
                  builder.setWidth(meta.width);
                  builder.setHeight(meta.height);
                  builder.setSize("original");
                  for(String transform : availableTransforms) {
                     builder.addTransformsBuilder()
                             .setSize(transform)
                             .setType(typeFromExtension(ext))
                             .setUrl(transformPath + meta.sourceHash  + ext);
                  }
                  replaceImages.add(builder.build());
               } else {
                  replaceImages.add(image);
               }
            } catch(IOException ioe) {
               logger.error(String.format("Problem enabling image, '%s'", image.getOriginalSrc()), ioe);
               replaceImages.add(image);
            }
         }
         entry.clearImages();
         entry.addAllImages(replaceImages);
         if(replaceImages.size() > 0) {
            entry.setPrimaryImage(replaceImages.get(0));
         }
      }
   }

   /**
    * Enables a duster image with configured transforms to pre-generate.
    * @param image The image.
    * @return The image hash, or {@code null} on error.
    * @throws InvalidURIException if image src is invalid.
    * @throws IOException on HTTP error.
    */
   public ImageMeta enableImage(final ClientProtos.WireMessage.Image image) throws IOException {

      Timer.Context ctx = pings.time();
      try {
         Response response = httpClient.send(buildPingRequest(image));
         String responseBody = response.getBody().toStringUtf8();
         switch(response.statusCode) {
            case 200:
            case 202:
               String[] vals = responseBody.split(",");
               if(vals.length < 4) {
                  logger.info(String.format("Duster ping failed with invalid response for '%s' (%s)", image.getOriginalSrc(), responseBody));
                  errors.mark();
                  return null;
               } else {
                  logger.info(String.format("Duster pinged with response '%s' for '%s'", response.getBody().toStringUtf8(), image.getOriginalSrc()));
                  int width = Integer.parseInt(vals[0].trim());
                  int height = Integer.parseInt(vals[1].trim());
                  String sourceHash = vals[2].trim();
                  String contentHash = vals[3].trim();
                  double aspect = (double)width / (double)height;
                  if(width < minWidth || height < minHeight || aspect < minAspect || aspect > maxAspect) {
                     logger.info(String.format("Duster image is not within constraints for '%s'", image.getOriginalSrc()));
                     skipped.mark();
                     return null;
                  } else {
                     return new ImageMeta(width, height, sourceHash, contentHash);
                  }
               }
            default:
               logger.error(String.format("Duster ping failed with code '%s' (%s)", response.statusCode, responseBody));
               return null;
         }
      } finally {
         ctx.stop();
      }
   }

   /**
    * Builds the duster ping request to check the image and pre-generate generate any transforms.
    * @param image The image.
    * @return The request to send.
    * @throws InvalidURIException if the URL is invalid.
    */
   private Request buildPingRequest(final ClientProtos.WireMessage.Image image) throws InvalidURIException {
      try {
         String url = URIEncoder.recodeURL(imageDimensionURL + "?" + pregenerateTransformQuery + "&minX=" + minWidth + "&minY=" + minHeight + "&src=" + image.getOriginalSrc());
         return new GetRequestBuilder(url).addHeader(BasicAuthScheme.AUTH_HEADER, this.auth).create();
      } catch(URISyntaxException ue) {
         throw new InvalidURIException(String.format("Invalid URI for '%s'", image.getUrl()), ue);
      }
   }

   private final String imageDimensionURL;
   private final String auth;
   private final int minWidth;
   private final int minHeight;
   private final double minAspect;
   private final double maxAspect;
   private final AsyncClient httpClient;
   private final Logger logger;
   private final ImmutableList<String> availableTransforms;
   private final String pregenerateTransformQuery;
   private final String transformPath;

   @Override
   public Map<String, Metric> getMetrics() {
      return ImmutableMap.of("pings", pings, "errors", errors, "skipped", skipped);
   }

   /**
    * Times all pings.
    */
   private final Timer pings = new Timer();

   /**
    * Records all errors.
    */
   private final Meter errors = new Meter();

   /**
    * Records all skipped images.
    */
   private final Meter skipped = new Meter();

   /**
    * Determines if an extension is allowed.
    * @param ext The extension.
    * @return Is the extension allowed?
    */
   private static boolean isExtensionAllowed(final String ext) {
      return ext.isEmpty() || allowedExtensions.contains(ext.toLowerCase());
   }

   /**
    * Gets the image type from the extension.
    * @param ext
    * @return
    */
   private static ClientProtos.WireMessage.ImageType typeFromExtension(final String ext) {
      switch(ext.toLowerCase()) {
         case ".jpg":
         case ".jpeg":
         case ".jpe":
            return ClientProtos.WireMessage.ImageType.JPG;
         case ".gif":
            return ClientProtos.WireMessage.ImageType.GIF;
         case ".png":
            return ClientProtos.WireMessage.ImageType.PNG;
         default:
            return ClientProtos.WireMessage.ImageType.UNKNOWN;
      }
   }

   /**
    * The allowed image extensions.
    */
   private static ImmutableSet<String> allowedExtensions = ImmutableSet.of(".jpg", ".jpeg", ".jpe", ".png", ".gif");

}