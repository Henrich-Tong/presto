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
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.decoder.separator.SeparatorDecoderModule.log;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Default field decoder for raw (byte) columns.
 */
public class SeparatorFieldDecoder
    implements FieldDecoder<List<String>> {

  @Override
  public Set<Class<?>> getJavaTypes() {
    return ImmutableSet.of(boolean.class, long.class, double.class, Slice.class);
  }

  @Override
  public final String getRowDecoderName() {
    return SeparatorRowDecoder.NAME;
  }

  @Override
  public String getFieldDecoderName() {
    return FieldDecoder.DEFAULT_FIELD_DECODER_NAME;
  }

  @Override
  public FieldValueProvider decode(List<String> values, DecoderColumnHandle columnHandle) {
    requireNonNull(columnHandle, "columnHandle is null");
    requireNonNull(values, "value is null");

    String mapping = columnHandle.getMapping();

    try {
      int index = Integer.parseInt(mapping);
      log.debug("--- index: " + index + "\tvalue: " + values.get(index));
      return new SeparatorValueProvider(values.get(index), columnHandle);
    } catch (Throwable t) {
      throw new THException(t.getMessage());
    }
  }

  @Override
  public String toString() {
    return format("FieldDecoder[%s/%s]", getRowDecoderName(), getFieldDecoderName());
  }

  public static class SeparatorValueProvider
      extends FieldValueProvider {
    protected final String value;
    protected final DecoderColumnHandle columnHandle;

    public SeparatorValueProvider(String separatorValue, DecoderColumnHandle columnHandle) {
      this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
      this.value = separatorValue;
    }

    @Override
    public final boolean accept(DecoderColumnHandle columnHandle) {
      return this.columnHandle.equals(columnHandle);
    }

    @Override
    public final boolean isNull() {
      return value == null;
    }

    @Override
    public boolean getBoolean() {
      if (isNull()) {
        return false;
      }
      return Boolean.parseBoolean(value);
    }

    @Override
    public long getLong() {
      if (isNull()) {
        return 0L;
      }
      return Long.parseLong(value);
    }

    @Override
    public double getDouble() {
      if (isNull()) {
        return 0.0d;
      }
      return Double.parseDouble(value);
    }

    @Override
    public Slice getSlice() {
      if (isNull()) {
        return Slices.EMPTY_SLICE;
      }

      if (isNull()) {
        return EMPTY_SLICE;
      }
      Slice slice = utf8Slice(value);
      if (isVarcharType(columnHandle.getType())) {
        slice = truncateToLength(slice, columnHandle.getType());
      }
      return slice;
    }
  }
}
