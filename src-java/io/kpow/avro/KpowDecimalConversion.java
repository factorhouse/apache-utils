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
 *
 * This is a modified version of org.apache.avro.Conversions.DecimalConversion
 */

package io.kpow.avro;

import org.apache.avro.*;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class KpowDecimalConversion extends Conversion<String> {

    @Override
    public Class<String> getConvertedType() {
        return String.class;
    }

    @Override
    public Schema getRecommendedSchema() {
        throw new UnsupportedOperationException("No recommended schema for decimal (scale is required)");
    }

    @Override
    public String getLogicalTypeName() {
        return "decimal";
    }

    @Override
    public String fromBytes(ByteBuffer value, Schema schema, LogicalType type) {
        int scale = ((LogicalTypes.Decimal) type).getScale();
        // always copy the bytes out because BigInteger has no offset/length ctor
        byte[] bytes = new byte[value.remaining()];
        value.duplicate().get(bytes);
        return new BigDecimal(new BigInteger(bytes), scale).toString();
    }

    @Override
    public ByteBuffer toBytes(String value, Schema schema, LogicalType type) {
        BigDecimal bdValue = validate((LogicalTypes.Decimal) type, new BigDecimal(value));
        return ByteBuffer.wrap(bdValue.unscaledValue().toByteArray());
    }

    @Override
    public String fromFixed(GenericFixed value, Schema schema, LogicalType type) {
        int scale = ((LogicalTypes.Decimal) type).getScale();
        return new BigDecimal(new BigInteger(value.bytes()), scale).toString();
    }

    @Override
    public GenericFixed toFixed(String value, Schema schema, LogicalType type) {
        BigDecimal bdValue = validate((LogicalTypes.Decimal) type, new BigDecimal(value));

        byte fillByte = (byte) (bdValue.signum() < 0 ? 0xFF : 0x00);
        byte[] unscaled = bdValue.unscaledValue().toByteArray();
        byte[] bytes = new byte[schema.getFixedSize()];
        int offset = bytes.length - unscaled.length;

        // Fill the front of the array and copy remaining with unscaled values
        Arrays.fill(bytes, 0, offset, fillByte);
        System.arraycopy(unscaled, 0, bytes, offset, bytes.length - offset);

        return new GenericData.Fixed(schema, bytes);
    }

    public static BigDecimal validate(final LogicalTypes.Decimal decimal, BigDecimal value) {
        final int scale = decimal.getScale();
        final int valueScale = value.scale();

        boolean scaleAdjusted = false;
        if (valueScale != scale) {
            try {
                value = value.setScale(scale, RoundingMode.UNNECESSARY);
                scaleAdjusted = true;
            } catch (ArithmeticException aex) {
                throw new AvroTypeException(
                        "Cannot encode decimal with scale " + valueScale + " as scale " + scale + " without rounding");
            }
        }

        int precision = decimal.getPrecision();
        int valuePrecision = value.precision();
        if (valuePrecision > precision) {
            if (scaleAdjusted) {
                throw new AvroTypeException("Cannot encode decimal with precision " + valuePrecision + " as max precision "
                        + precision + ". This is after safely adjusting scale from " + valueScale + " to required " + scale);
            } else {
                throw new AvroTypeException(
                        "Cannot encode decimal with precision " + valuePrecision + " as max precision " + precision);
            }
        }

        return value;
    }
}