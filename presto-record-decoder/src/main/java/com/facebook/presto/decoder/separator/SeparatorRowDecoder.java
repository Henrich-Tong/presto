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
import com.facebook.presto.decoder.RowDecoder;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.decoder.separator.SeparatorDecoderModule.log;

/**
 * Decoder for raw (direct byte) rows. All field decoders map bytes directly to Presto columns.
 */
public class SeparatorRowDecoder
    implements RowDecoder {
  public static final String NAME = "separator";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean decodeRow(byte[] data,
                           Map<String, String> dataMap,
                           Set<FieldValueProvider> fieldValueProviders,
                           List<DecoderColumnHandle> columnHandles,
                           Map<DecoderColumnHandle, FieldDecoder<?>> fieldDecoders) {
    String content = new String(data);
    List<String> values = SeparatorDecoderModule.SEPARATOR.splitToList(content);
    log.debug("--- content: " + content);

    for (DecoderColumnHandle columnHandle : columnHandles) {
      if (columnHandle.isInternal()) {
        continue;
      }

      @SuppressWarnings("unchecked")
      FieldDecoder<List<String>> decoder = (FieldDecoder<List<String>>) fieldDecoders.get(columnHandle);

      if (decoder != null) {
        fieldValueProviders.add(decoder.decode(values, columnHandle));
      }
    }

    return false;
  }
}
