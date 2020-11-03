// Copyright 2016 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.devtools.build.lib.bazel.repository.downloader;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.devtools.build.lib.analysis.BlazeVersionInfo;
import com.google.devtools.build.lib.buildeventstream.FetchEvent;
import com.google.devtools.build.lib.clock.Clock;
import com.google.devtools.build.lib.clock.JavaClock;
import com.google.devtools.build.lib.events.Event;
import com.google.devtools.build.lib.events.ExtendedEventHandler;
import com.google.devtools.build.lib.util.JavaSleeper;
import com.google.devtools.build.lib.util.Sleeper;
import com.google.devtools.build.lib.vfs.Path;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Semaphore;

/** HTTP implementation of {@link Downloader}. */
public class HttpDownloader implements Downloader {
  private static final int MAX_PARALLEL_DOWNLOADS = 8;
  private static final int BUFFER_SIZE = 8192;
  private static final Semaphore semaphore = new Semaphore(MAX_PARALLEL_DOWNLOADS, true);
  private static final ImmutableMap<String, String> REQUEST_HEADERS =
      new ImmutableMap.Builder()
          .put("Accept-Encoding", "gzip")
          .put("User-Agent", "Bazel/" + BlazeVersionInfo.instance().getReleaseName())
          .build();

  private float timeoutScaling = 1.0f;

  public HttpDownloader() {}

  public void setTimeoutScaling(float timeoutScaling) {
    this.timeoutScaling = timeoutScaling;
  }

  static Function<URL, ImmutableMap<String, String>> getHeaderFunction(
      Map<String, String> baseHeaders, Map<URI, Map<String, String>> additionalHeaders) {
    return url -> {
      try {
        if (additionalHeaders.containsKey(url.toURI())) {
          Map<String, String> newHeaders = new HashMap<>(baseHeaders);
          newHeaders.putAll(additionalHeaders.get(url.toURI()));
          return ImmutableMap.copyOf(newHeaders);
        }
      } catch (URISyntaxException e) {
        // If we can't convert the URL to a URI (because it is syntactically malformed), still
        // try to do the connection, not adding authentication information as we cannot look it
        // up.
      }

      return ImmutableMap.copyOf(baseHeaders);
    };
  }

  @Override
  public void download(
      List<URL> urls,
      Map<URI, Map<String, String>> authHeaders,
      Optional<Checksum> checksum,
      String canonicalId,
      Path destination,
      ExtendedEventHandler eventHandler,
      Map<String, String> clientEnv,
      Optional<String> type)
      throws IOException, InterruptedException {
    Clock clock = new JavaClock();
    Sleeper sleeper = new JavaSleeper();
    Locale locale = Locale.getDefault();
    ProxyHelper proxyHelper = new ProxyHelper(clientEnv);
    HttpConnector connector =
        new HttpConnector(locale, eventHandler, proxyHelper, sleeper, timeoutScaling);
    ProgressInputStream.Factory progressInputStreamFactory =
        new ProgressInputStream.Factory(locale, clock, eventHandler);
    HttpStream.Factory httpStreamFactory = new HttpStream.Factory(progressInputStreamFactory);

    // Iterate over urls and download the file falling back to the next url if previous failed,
    // while reporting progress to the CLI.
    boolean success = false;

    List<IOException> ioExceptions = ImmutableList.of();

    for (URL url : urls) {
      semaphore.acquire();

      Preconditions.checkArgument(
          HttpUtils.isUrlSupportedByDownloader(url), "unsupported protocol: %s", url);

      try {
        int retries = 0;
        while (!success && retries++ < 3) {
          try (HttpStream payload =
                  httpStreamFactory.create(
                      connector.connect(url, getHeaderFunction(REQUEST_HEADERS, authHeaders)),
                      url,
                      checksum,
                      type);
              OutputStream out = destination.getOutputStream()) {
            copyStream(payload, out);
            success = true;
          } catch (IOException e) {
            if (ioExceptions.isEmpty()) {
              ioExceptions = new ArrayList<>(1);
            }
            ioExceptions.add(e);
            eventHandler.handle(
                Event.warn(
                    "Download from " + url + " failed: " + e.getClass() + " " + e.getMessage()));
          }
        }
      } finally {
        semaphore.release();
        eventHandler.post(new FetchEvent(url.toString(), success));
      }

      if (success) {
        break;
      }
    }

    if (!success) {
      final IOException exception =
          new IOException(
              "Error downloading "
                  + urls
                  + " to "
                  + destination
                  + (ioExceptions.isEmpty()
                      ? ""
                      : ": " + Iterables.getLast(ioExceptions).getMessage()));

      for (IOException cause : ioExceptions) {
        exception.addSuppressed(cause);
      }

      throw exception;
    }
  }

  private static void copyStream(InputStream payload, OutputStream out)
      throws InterruptedException, IOException {
    byte[] buf = new byte[BUFFER_SIZE];
    while (true) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      int r = payload.read(buf);
      if (r == -1) {
        break;
      }
      out.write(buf, 0, r);
    }
  }
}
