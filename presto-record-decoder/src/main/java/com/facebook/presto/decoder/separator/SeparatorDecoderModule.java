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

import com.facebook.presto.decoder.DecoderRegistry;
import com.google.common.base.Splitter;
import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.log.Logger;

import static com.facebook.presto.decoder.DecoderModule.bindFieldDecoder;
import static com.facebook.presto.decoder.DecoderModule.bindRowDecoder;

/**
 * Raw decoder guice module.
 */
public class SeparatorDecoderModule
        implements Module
{

    static final Logger log = Logger.get(SeparatorDecoderModule.class);

    static Splitter SEPARATOR = Splitter.on("|").trimResults();

    @Override
    public void configure(Binder binder)
    {
        bindRowDecoder(binder, SeparatorRowDecoder.class);
        bindFieldDecoder(binder, SeparatorFieldDecoder.class);
    }
}
