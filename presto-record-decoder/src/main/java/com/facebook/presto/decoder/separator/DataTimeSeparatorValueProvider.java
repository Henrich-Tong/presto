package com.facebook.presto.decoder.separator;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.json.JsonFieldDecoder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;

/**
 * Created by tongh on 2017/6/14
 */
public abstract class DataTimeSeparatorValueProvider extends SeparatorFieldDecoder.SeparatorValueProvider {

  protected DataTimeSeparatorValueProvider(String value, DecoderColumnHandle columnHandle) {
    super(value, columnHandle);
  }

  @Override
  public boolean getBoolean() {
    throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, "conversion to boolean not supported");
  }

  @Override
  public double getDouble() {
    throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, "conversion to double not supported");
  }

  @Override
  public final long getLong() {
    long millis = getMillis();

    Type type = columnHandle.getType();
    if (type.equals(DATE)) {
      return TimeUnit.MILLISECONDS.toDays(millis);
    }
    if (type.equals(TIMESTAMP) || type.equals(TIME)) {
      return millis;
    }
    if (type.equals(TIMESTAMP_WITH_TIME_ZONE) || type.equals(TIME_WITH_TIME_ZONE)) {
      return packDateTimeWithZone(millis, 0);
    }

    return millis;
  }

  /**
   * @return epoch milliseconds in UTC
   */
  protected abstract long getMillis();
}