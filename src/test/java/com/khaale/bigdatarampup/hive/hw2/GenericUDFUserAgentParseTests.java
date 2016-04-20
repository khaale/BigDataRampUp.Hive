package com.khaale.bigdatarampup.hive.hw2;

import junit.framework.TestCase;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

/**
 * Created by Aleksander_Khanteev on 4/16/2016.
 */
@RunWith(Parameterized.class)
public class GenericUDFUserAgentParseTests {

    private final String userAgent;
    private final String uaType;
    private final String uaFamily;
    private final String osName;
    private final String device;

    @Parameterized.Parameters(name = "{index}: UA \"{0}\"")
    public static Collection userAgents() {
        return Arrays.asList(new Object[][] {
                { "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36", "Browser", "Chrome", "Windows 7", "Computer" },
                { "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1", "Browser", "Firefox", "Windows 7", "Computer"},
                { "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko", "Browser", "Internet Explorer", "Windows 7", "Computer" },
                { "Mozilla/5.0 (Windows; U; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)", "Browser", "Internet Explorer", "Windows XP", "Computer" },
                { "Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16", "Browser", "Opera", "Ubuntu", "Computer" },
                { "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A", "Browser", "Safari", "Mac OS X", "Computer"},
                { "Mozilla/5.0 (Linux; U; Android 2.3.4; en-us; T-Mobile myTouch 3G Slide Build/GRI40) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1", "Browser (mobile)", "Safari", "Android 2.x", "Mobile"}
        });
    }

    public GenericUDFUserAgentParseTests(
            String userAgent,
            String uaType,
            String uaFamily,
            String osName,
            String device
    ) {

        this.userAgent = userAgent;
        this.uaType = uaType;
        this.uaFamily = uaFamily;
        this.osName = osName;
        this.device = device;
    }


    @Test
    public void shouldParseUserAgents() throws HiveException {
        //arrange
        ObjectInspector inputOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);

        //act
        GenericUDFUserAgentParse sut = new GenericUDFUserAgentParse();

        sut.initialize(new ObjectInspector[] {inputOI});
        Object result = sut.evaluate(new GenericUDF.DeferredObject[]{new GenericUDF.DeferredJavaObject(new Text(userAgent))});

        //assert
        assertThat(result, instanceOf(String[].class));
        String[] parsedUa = (String[])result;
        assertThat(parsedUa[0], is(uaType));
        assertThat(parsedUa[1], is(uaFamily));
        assertThat(parsedUa[2], is(osName));
        assertThat(parsedUa[3], is(device));

    }
}
