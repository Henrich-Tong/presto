/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.decoder.separator;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.facebook.presto.decoder.separator.SeparatorDecoderModule.log;
import static java.util.Objects.requireNonNull;

/**
 * RFC 2822 date format decoder.
 * <p/>
 * Uses hardcoded UTC timezone and english locale.
 */
public class RFC2822SeparatorFieldDecoder
    extends SeparatorFieldDecoder {
  @VisibleForTesting
  static final String NAME = "rfc2822";

  /**
   * Todo - configurable time zones and locales.
   */
  @VisibleForTesting
//  static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH).withZoneUTC();
  static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withLocale(Locale.ENGLISH).withZoneUTC();

  @Override
  public Set<Class<?>> getJavaTypes() {
    return ImmutableSet.of(long.class, Slice.class);
  }

  @Override
  public String getFieldDecoderName() {
    return NAME;
  }

  @Override
  public FieldValueProvider decode(List<String> values, DecoderColumnHandle columnHandle) {
    requireNonNull(columnHandle, "columnHandle is null");
    requireNonNull(values, "value is null");

    String mapping = columnHandle.getMapping();

    try {
      int index = Integer.parseInt(mapping);
      log.debug("--- index: " + index + "\tvalue: " + values.get(index));
      return new RFC2822SeparatorValueProvider(values.get(index), columnHandle);
    } catch (Throwable t) {
      throw new THException(t.getMessage());
    }
  }

  public static class RFC2822SeparatorValueProvider
      extends DataTimeSeparatorValueProvider {
    public RFC2822SeparatorValueProvider(String value, DecoderColumnHandle columnHandle) {
      super(value, columnHandle);
    }

    @Override
    protected long getMillis() {
      if (isNull()) {
        return 0L;
      }

      try {
        return Long.parseLong(value);
      } catch (Throwable t) {
        return FORMATTER.parseMillis(value);
      }
    }
  }
}
