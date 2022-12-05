package org.apache.doris.statistics;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;

class HistogramTest {
    private Histogram histogramUnderTest;

    @BeforeEach
    void setUp() throws Exception {
        String json = "{\"max_bucket_size\":128,\"bucket_size\":5,\"sample_rate\":1.0,\"buckets\":"
                + "[{\"lower\":\"2022-09-21 17:30:29\",\"upper\":\"2022-09-21 22:30:29\","
                + "\"count\":9,\"pre_sum\":0,\"ndv\":1},"
                + "{\"lower\":\"2022-09-22 17:30:29\",\"upper\":\"2022-09-22 22:30:29\","
                + "\"count\":10,\"pre_sum\":9,\"ndv\":1},"
                + "{\"lower\":\"2022-09-23 17:30:29\",\"upper\":\"2022-09-23 22:30:29\","
                + "\"count\":9,\"pre_sum\":19,\"ndv\":1},"
                + "{\"lower\":\"2022-09-24 17:30:29\",\"upper\":\"2022-09-24 22:30:29\","
                + "\"count\":9,\"pre_sum\":28,\"ndv\":1},"
                + "{\"lower\":\"2022-09-25 17:30:29\",\"upper\":\"2022-09-25 22:30:29\","
                + "\"count\":9,\"pre_sum\":37,\"ndv\":1}]}";
        histogramUnderTest = Histogram.deserializeFromJson(json, Type.fromPrimitiveType(PrimitiveType.DATETIME));
        if (histogramUnderTest == null) {
            Assertions.fail();
        }
    }

    @Test
    void testFindBucket() throws Exception {
        // Setup
        LiteralExpr key1 = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr key2 = LiteralExpr.create("2022-09-23 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));

        // Run the test
        Bucket bucket1 = histogramUnderTest.findBucket(key1);
        Bucket bucket2 = histogramUnderTest.findBucket(key2);

        // Verify the results
        Assertions.assertEquals(1, bucket1.getNdv());
        Assertions.assertEquals(1, bucket2.getNdv());
        Assertions.assertEquals(9, bucket1.getCount());
        Assertions.assertEquals(9, bucket2.getCount());
        Assertions.assertEquals(0, bucket1.getPreSum());
        Assertions.assertEquals(19, bucket2.getPreSum());

        LiteralExpr lower1 = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr lower2 = LiteralExpr.create("2022-09-23 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr upper1 = LiteralExpr.create("2022-09-21 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr upper2 = LiteralExpr.create("2022-09-23 22:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        Assertions.assertEquals(lower1, bucket1.getLower());
        Assertions.assertEquals(lower2, bucket2.getLower());
        Assertions.assertEquals(upper1, bucket1.getUpper());
        Assertions.assertEquals(upper2, bucket2.getUpper());
    }

    @Test
    void testRangeCount() throws Exception {
        // Setup
        LiteralExpr lower = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr upper = LiteralExpr.create("2022-09-23 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));

        // Run the test
        long count1 = histogramUnderTest.rangeCount(lower, true, upper, true);
        long count2 = histogramUnderTest.rangeCount(lower, true, upper, false);
        long count3 = histogramUnderTest.rangeCount(lower, false, upper, false);
        long count4 = histogramUnderTest.rangeCount(lower, false, upper, true);
        long count5 = histogramUnderTest.rangeCount(null, true, upper, true);
        long count6 = histogramUnderTest.rangeCount(lower, true, null, true);
        long count7 = histogramUnderTest.rangeCount(null, true, null, true);

        // Verify the results
        Assertions.assertEquals(28L, count1);
        Assertions.assertEquals(19L, count2);
        Assertions.assertEquals(10L, count3);
        Assertions.assertEquals(19L, count4);
        Assertions.assertEquals(28L, count5);
        Assertions.assertEquals(46L, count6);
        Assertions.assertEquals(46L, count7);
    }

    @Test
    void testDeserializeFromJson() throws Exception {
        Type dataType = histogramUnderTest.getDataType();
        Assertions.assertTrue(dataType.isDatetime());

        int maxBucketSize = histogramUnderTest.getMaxBucketSize();
        Assertions.assertEquals(128, maxBucketSize);

        int bucketSize = histogramUnderTest.getBucketSize();
        Assertions.assertEquals(5, bucketSize);

        float sampleRate = histogramUnderTest.getSampleRate();
        Assertions.assertEquals(1.0, sampleRate);

        List<Bucket> buckets = histogramUnderTest.getBuckets();
        Assertions.assertEquals(5, buckets.size());

        LiteralExpr lower = LiteralExpr.create("2022-09-21 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        LiteralExpr upper = LiteralExpr.create("2022-09-25 17:30:29",
                Objects.requireNonNull(Type.fromPrimitiveType(PrimitiveType.DATETIME)));
        long count = histogramUnderTest.rangeCount(lower, false, upper, false);
        Assertions.assertEquals(28, count);
    }
}
