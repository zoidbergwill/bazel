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

import static com.google.common.truth.Truth.assertThat;
import static com.google.devtools.build.lib.bazel.repository.downloader.DownloaderTestUtils.makeUrl;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Optional;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.devtools.build.lib.bazel.repository.cache.RepositoryCache.KeyType;
import com.google.devtools.build.lib.bazel.repository.downloader.RetryingInputStream.Reconnector;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.stubbing.Answer;

/** Integration tests for {@link HttpStream.Factory} and friends. */
@RunWith(JUnit4.class)
public class HttpStreamTest {

  private static final byte[] TEST_DATA = new byte[70001];
  private static final Checksum TEST_CHECKSUM;
  private static final Checksum BAD_CHECKSUM =
      Checksum.fromString(
          KeyType.SHA256, "0000000000000000000000000000000000000000000000000000000000000000");
  private static final URL AURL = makeUrl("http://doodle.example");

  private final HttpURLConnection connection = mock(HttpURLConnection.class);
  private final Reconnector reconnector = mock(Reconnector.class);
  private final ProgressInputStream.Factory progress = mock(ProgressInputStream.Factory.class);
  private final HttpStream.Factory streamFactory = new HttpStream.Factory(progress);

  private int nRetries;

  static {
    new Random().nextBytes(TEST_DATA);
    TEST_CHECKSUM =
        Checksum.fromString(KeyType.SHA256, Hashing.sha256().hashBytes(TEST_DATA).toString());
  }

  @Before
  public void before() throws Exception {
    nRetries = 0;

    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(TEST_DATA));
    when(progress.create(any(InputStream.class), any(), any(URL.class)))
        .thenAnswer((Answer<InputStream>) invocation -> (InputStream) invocation.getArguments()[0]);
  }

  @Test
  public void noChecksum_readsOk() throws Exception {
    try (HttpStream stream =
        streamFactory.create(connection, AURL, Optional.absent(), reconnector)) {
      assertThat(ByteStreams.toByteArray(stream)).isEqualTo(TEST_DATA);
    }
  }

  @Test
  public void dataWithValidChecksum_timesOutInCreateRetriesOk() throws Exception {
    InputStream inputStream = mock(ByteArrayInputStream.class);
    InputStream realInputStream = new ByteArrayInputStream(TEST_DATA);

    doAnswer(
            (Answer<Integer>)
                invocation -> {
                  Object[] args = invocation.getArguments();

                  if (nRetries++ == 0) {
                    throw new SocketTimeoutException();
                  } else {
                    return realInputStream.read((byte[]) args[0], (int) args[1], (int) args[2]);
                  }
                })
        .when(inputStream)
        .read(any(), anyInt(), anyInt());
    when(reconnector.connect(any(), any())).thenReturn(connection);
    when(connection.getInputStream()).thenReturn(inputStream);
    when(connection.getHeaderField("Accept-Ranges")).thenReturn("bytes");
    try (HttpStream stream =
        streamFactory.create(connection, AURL, Optional.of(TEST_CHECKSUM), reconnector)) {
      assertThat(ByteStreams.toByteArray(stream)).isEqualTo(TEST_DATA);
    }
  }

  @Test
  public void dataWithValidChecksum_timesOutRepeatedly() throws Exception {
    InputStream inputStream = mock(ByteArrayInputStream.class);

    doAnswer(
            (Answer<Integer>)
                invocation -> {
                  ++nRetries;
                  throw new SocketTimeoutException();
                })
        .when(inputStream)
        .read(any(), anyInt(), anyInt());
    when(reconnector.connect(any(), any())).thenReturn(connection);
    when(connection.getInputStream()).thenReturn(inputStream);
    when(connection.getHeaderField("Accept-Ranges")).thenReturn("bytes");

    HttpStream stream =
        streamFactory.create(connection, AURL, Optional.of(TEST_CHECKSUM), reconnector);
    assertThrows(SocketTimeoutException.class, () -> ByteStreams.exhaust(stream));
    assertThat(nRetries).isGreaterThan(3); // RetryingInputStream.MAX_RESUMES
  }

  @Test
  public void dataWithValidChecksum_readsOk() throws Exception {
    try (HttpStream stream =
        streamFactory.create(connection, AURL, Optional.of(TEST_CHECKSUM), reconnector)) {
      assertThat(ByteStreams.toByteArray(stream)).isEqualTo(TEST_DATA);
    }
  }

  @Test
  public void dataWithInvalidChecksum_throwsIOExceptionOnExhaust() throws Exception {
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(TEST_DATA));
    HttpStream stream =
        streamFactory.create(connection, AURL, Optional.of(BAD_CHECKSUM), reconnector);
    IOException e =
        assertThrows(UnrecoverableHttpException.class, () -> ByteStreams.exhaust(stream));
    assertThat(e).hasMessageThat().contains("Checksum");
  }

  @Test
  public void httpServerSaidGzippedButNotGzipped_throwsZipExceptionInCreate() {
    when(connection.getURL()).thenReturn(AURL);
    when(connection.getContentEncoding()).thenReturn("gzip");
    assertThrows(
        ZipException.class,
        () -> streamFactory.create(connection, AURL, Optional.absent(), reconnector));
  }

  @Test
  public void javascriptGzippedInTransit_automaticallyGunzips() throws Exception {
    when(connection.getURL()).thenReturn(AURL);
    when(connection.getContentEncoding()).thenReturn("x-gzip");
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(gzipData(TEST_DATA)));
    try (HttpStream stream =
        streamFactory.create(connection, AURL, Optional.absent(), reconnector)) {
      assertThat(ByteStreams.toByteArray(stream)).isEqualTo(TEST_DATA);
    }
  }

  @Test
  public void serverSaysTarballPathIsGzipped_doesntAutomaticallyGunzip() throws Exception {
    byte[] gzData = gzipData(TEST_DATA);
    when(connection.getURL()).thenReturn(new URL("http://doodle.example/foo.tar.gz"));
    when(connection.getContentEncoding()).thenReturn("gzip");
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(gzData));
    try (HttpStream stream =
        streamFactory.create(connection, AURL, Optional.absent(), reconnector)) {
      assertThat(ByteStreams.toByteArray(stream)).isEqualTo(gzData);
    }
  }

  @Test
  public void threadInterrupted_haltsReadingAndThrowsInterrupt() throws Exception {
    final AtomicBoolean wasInterrupted = new AtomicBoolean();
    Thread thread =
        new Thread(
            () -> {
              try (HttpStream stream =
                  streamFactory.create(connection, AURL, Optional.absent(), reconnector)) {
                stream.read();
                Thread.currentThread().interrupt();
                stream.read();
                fail();
              } catch (InterruptedIOException expected) {
                wasInterrupted.set(true);
              } catch (IOException ignored) {
                // ignored
              }
            });
    thread.start();
    thread.join();
    assertThat(wasInterrupted.get()).isTrue();
  }

  private static byte[] gzipData(byte[] bytes) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (InputStream input = new ByteArrayInputStream(bytes);
        OutputStream output = new GZIPOutputStream(baos)) {
      ByteStreams.copy(input, output);
    }
    return baos.toByteArray();
  }

  @Test
  public void tarballHasNoFormatAndTypeIsGzipped_doesntAutomaticallyGunzip() throws Exception {
    byte[] gzData = gzipData(TEST_DATA);
    when(connection.getURL()).thenReturn(new URL("http://doodle.example/foo"));
    when(connection.getContentEncoding()).thenReturn("gzip");
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(gzData));
    try (HttpStream stream =
        streamFactory.create(
            connection, AURL, Optional.absent(), reconnector, Optional.of("tgz"))) {
      assertThat(ByteStreams.toByteArray(stream)).isEqualTo(gzData);
    }
  }

  @Test
  public void tarballHasNoFormatAndTypeIsGzippedAndHasMultipleExtensions_doesntAutomaticallyGunzip()
      throws Exception {
    // Similar to tarballHasNoFormatAndTypeIsGzipped_doesntAutomaticallyGunzip but also
    // checks if the private method typeIsGZIP can handle separation of file extensions.
    byte[] gzData = gzipData(TEST_DATA);
    when(connection.getURL()).thenReturn(new URL("http://doodle.example/foo"));
    when(connection.getContentEncoding()).thenReturn("gzip");
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(gzData));
    try (HttpStream stream =
        streamFactory.create(
            connection, AURL, Optional.absent(), reconnector, Optional.of("tar.gz"))) {
      assertThat(ByteStreams.toByteArray(stream)).isEqualTo(gzData);
    }
  }

  @Test
  public void tarballHasNoFormatAndTypeIsNotGzipped_automaticallyGunzip() throws Exception {
    when(connection.getURL()).thenReturn(new URL("http://doodle.example/foo"));
    when(connection.getContentEncoding()).thenReturn("gzip");
    when(connection.getInputStream()).thenReturn(new ByteArrayInputStream(gzipData(TEST_DATA)));
    try (HttpStream stream =
        streamFactory.create(
            connection, AURL, Optional.absent(), reconnector, Optional.of("tar"))) {
      assertThat(ByteStreams.toByteArray(stream)).isEqualTo(TEST_DATA);
    }
  }
}
