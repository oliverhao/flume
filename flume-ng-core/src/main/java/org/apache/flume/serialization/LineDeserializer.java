/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.serialization;

import com.google.common.collect.Lists;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.TimestampInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A deserializer that parses text lines from a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class LineDeserializer implements EventDeserializer {

  private static final Logger logger = LoggerFactory.getLogger(LineDeserializer.class);

  private final ResettableInputStream in;
  private final Charset outputCharset;
  private final int maxLineLength;
  private volatile boolean isOpen;

  public static final String OUT_CHARSET_KEY = "outputCharset";
  public static final String CHARSET_DFLT = "UTF-8";

  public static final String MAXLINE_KEY = "maxLineLength";
  public static final int MAXLINE_DFLT = 2048;

  private boolean multiline;
  private Pattern multilinePattern;
  private String multilinePatternBelong;
  private boolean multilinePatternMatched;
  private long multilineEventTimeoutSecs;
  private int multilineMaxBytes;
  private int multilineMaxLines;
  private Event bufferEvent;

  private static final byte BYTE_NL = (byte) 10;
  private static final byte BYTE_CR = (byte) 13;

  LineDeserializer(Context context, ResettableInputStream in) {
    this.in = in;
    this.outputCharset = Charset.forName(
        context.getString(OUT_CHARSET_KEY, CHARSET_DFLT));
    this.maxLineLength = context.getInteger(MAXLINE_KEY, MAXLINE_DFLT);
    this.isOpen = true;
    this.multiline = context.getBoolean("multiline", false);
    this.multilinePattern = Pattern.compile(context.getString("multilinePattern", ""));
    this.multilinePatternBelong = context.getString("multilinePatternBelong", "previous");
    this.multilinePatternMatched = context.getBoolean("multilinePatternMatched", true);
    this.multilineMaxBytes = context.getInteger("multilineMaxBytes", 10240);
    this.multilineEventTimeoutSecs = context.getInteger("multilineEventTimeoutSecs", 60);
    this.multilineMaxLines = context.getInteger("multilineMaxLines", 500);
    this.bufferEvent = null;
  }

  /**
   * Reads a line from a file and returns an event
   * @return Event containing parsed line
   * @throws IOException
   */
  @Override
  public Event readEvent() throws IOException {
    ensureOpen();
    String line = readLine();
    if (line == null) {
      return null;
    } else {
      return EventBuilder.withBody(line, outputCharset);
    }
  }

  public boolean needFlushTimeoutEvent() {
    return true;
    /*
    if (bufferEvent != null) {
      long now = System.currentTimeMillis();
      long eventTime = Long.parseLong(
          bufferEvent.getHeaders().get(TimestampInterceptor.Constants.TIMESTAMP));
      if (multilineEventTimeoutSecs > 0 && (now - eventTime) > multilineEventTimeoutSecs * 1000) {
        return true;
      }
    }
    return false;
    */
  }

  /**
   * Batch line read
   * @param numEvents Maximum number of events to return.
   * @return List of events containing read lines
   * @throws IOException
   */
  @Override
  public List<Event> readEvents(int numEvents) throws IOException {
    ensureOpen();
    List<Event> events = Lists.newLinkedList();
    if (this.multiline) {
      //if (raf != null) { // when file has not closed yet
      boolean match = this.multilinePatternMatched;
      while (events.size() < numEvents) {
        String strLine = readLine();
        if (strLine == null) {
          break;
        }
        LineResult line = new LineResult(true, strLine.getBytes());

        Event event = null;
        switch (this.multilinePatternBelong) {
          case "next":
            event = readMultilineEventNext(line, match);
            break;
          case "previous":
            event = readMultilineEventPre(line, match);
            break;
          default:
            break;
        }
        if (event != null) {
          events.add(event);
        }
        if (bufferEvent != null) {
          if (bufferEvent.getBody().length >= multilineMaxBytes
              || Integer.parseInt(bufferEvent.getHeaders().get("lineCount")) == multilineMaxLines) {
            flushBufferEvent(events);
          }
        }
      }
    }
    if (needFlushTimeoutEvent()) {
      flushBufferEvent(events);
      //}
    } else {
      for (int i = 0; i < numEvents; i++) {
        Event event = readEvent();
        if (event != null) {
          events.add(event);
        } else {
          break;
        }
      }
    }
    return events;
  }

  @Override
  public void mark() throws IOException {
    ensureOpen();
    in.mark();
  }

  @Override
  public void reset() throws IOException {
    ensureOpen();
    in.reset();
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      reset();
      in.close();
      isOpen = false;
    }
  }

  private void ensureOpen() {
    if (!isOpen) {
      throw new IllegalStateException("Serializer has been closed");
    }
  }

  // TODO: consider not returning a final character that is a high surrogate
  // when truncating
  private String readLine() throws IOException {
    StringBuilder sb = new StringBuilder();
    int c;
    int readChars = 0;
    while ((c = in.readChar()) != -1) {
      readChars++;

      // FIXME: support \r\n
      if (c == '\n') {
        break;
      }

      sb.append((char)c);

      if (readChars >= maxLineLength) {
        logger.warn("Line length exceeds max ({}), truncating line!",
            maxLineLength);
        break;
      }
    }

    if (readChars > 0) {
      return sb.toString();
    } else {
      return null;
    }
  }

  private Event readMultilineEventPre(LineResult line, boolean match)
      throws IOException {
    Event event = null;
    Matcher m = multilinePattern.matcher(new String(line.line));
    boolean find = m.find();
    match = (find && match) || (!find && !match);
    byte[] lineBytes = toOriginBytes(line);
    if (match) {
      /** If matched, merge it to the buffer event. */
      mergeEvent(line);
    } else {
      /**
       * If not matched, this line is not part of previous event when the buffer event is not null.
       * Then create a new event with buffer event's message and put the current line into the
       * cleared buffer event.
       */
      if (bufferEvent != null) {
        event = EventBuilder.withBody(bufferEvent.getBody());
      }
      bufferEvent = null;
      bufferEvent = EventBuilder.withBody(lineBytes);
      if (line.lineSepInclude) {
        bufferEvent.getHeaders().put("lineCount", "1");
      } else {
        bufferEvent.getHeaders().put("lineCount", "0");
      }
      long now = System.currentTimeMillis();
      bufferEvent.getHeaders().put(TimestampInterceptor.Constants.TIMESTAMP, Long.toString(now));
    }
    return event;
  }

  private Event readMultilineEventNext(LineResult line, boolean match)
      throws IOException {
    Event event = null;
    Matcher m = multilinePattern.matcher(new String(line.line));
    boolean find = m.find();
    match = (find && match) || (!find && !match);
    if (match) {
      /** If matched, merge it to the buffer event. */
      mergeEvent(line);
    } else {
      /**
       * If not matched, this line is not part of next event. Then merge the current line into the
       * buffer event and create a new event with the merged message.
       */
      mergeEvent(line);
      event = EventBuilder.withBody(bufferEvent.getBody());
      bufferEvent = null;
    }
    return event;
  }

  private void mergeEvent(LineResult line) {
    byte[] lineBytes = toOriginBytes(line);
    if (bufferEvent != null) {
      byte[] bufferBytes = bufferEvent.getBody();
      byte[] mergedBytes = concatByteArrays(bufferBytes, 0, bufferBytes.length,
          lineBytes, 0, lineBytes.length);
      int lineCount = Integer.parseInt(bufferEvent.getHeaders().get("lineCount"));
      bufferEvent.setBody(mergedBytes);
      if (line.lineSepInclude) {
        bufferEvent.getHeaders().put("lineCount", String.valueOf(lineCount + 1));
      }
    } else {
      bufferEvent = EventBuilder.withBody(lineBytes);
      bufferEvent.getHeaders().put("multiline", "true");
      if (line.lineSepInclude) {
        bufferEvent.getHeaders().put("lineCount", "1");
      } else {
        bufferEvent.getHeaders().put("lineCount", "0");
      }
    }
    long now = System.currentTimeMillis();
    bufferEvent.getHeaders().put(TimestampInterceptor.Constants.TIMESTAMP, Long.toString(now));
  }

  private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA,
      byte[] b, int startIdxB, int lenB) {
    byte[] c = new byte[lenA + lenB];
    System.arraycopy(a, startIdxA, c, 0, lenA);
    System.arraycopy(b, startIdxB, c, lenA, lenB);
    return c;
  }

  private void flushBufferEvent(List<Event> events) {
    if (bufferEvent != null) {
      Event event = EventBuilder.withBody(bufferEvent.getBody());
      events.add(event);
      bufferEvent = null;
    }
  }

  private byte[] toOriginBytes(LineResult line) {
    byte[] originBytes = null;
    if (line.lineSepInclude) {
      originBytes  = new byte[line.line.length + 1];
      System.arraycopy(line.line, 0, originBytes , 0, line.line.length);
      originBytes [line.line.length] = BYTE_NL;
      return originBytes;
    }
    return line.line;
  }

  public static class Builder implements EventDeserializer.Builder {

    @Override
    public EventDeserializer build(Context context, ResettableInputStream in) {
      return new LineDeserializer(context, in);
    }
  }

  private class LineResult {
    final boolean lineSepInclude;
    final byte[] line;

    public LineResult(boolean lineSepInclude, byte[] line) {
      super();
      this.lineSepInclude = lineSepInclude;
      this.line = line;
    }
  }
}
