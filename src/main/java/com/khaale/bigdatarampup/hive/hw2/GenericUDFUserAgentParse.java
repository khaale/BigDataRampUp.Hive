package com.khaale.bigdatarampup.hive.hw2;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Aleksander_Khanteev on 4/16/2016.
 */
@Description(
        name = "agent_parse_udf",
        value = " _FUNC_(user_agent) returns parsed UA information: { ua_type, ua_family, os_name, device }")
public class GenericUDFUserAgentParse extends GenericUDF {

    private PrimitiveObjectInspector inputInspector;

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        inputInspector = (PrimitiveObjectInspector) objectInspectors[0];
        ObjectInspector stringInspector = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);

        List<String> outputFields = Arrays.asList("ua_type","ua_family", "os_name", "device");
        List<ObjectInspector> outputInspectors = Arrays.asList(stringInspector, stringInspector, stringInspector, stringInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(outputFields, outputInspectors);
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        String input = (String)inputInspector.getPrimitiveJavaObject(deferredObjects[0].get());
        UserAgent ua = UserAgent.parseUserAgentString(input);

        String uaType = ua.getBrowser() != null ? ua.getBrowser().getBrowserType().getName() : null;
        String uaFamily = ua.getBrowser() != null ? ua.getBrowser().getGroup().getName() : null;
        String osName = ua.getOperatingSystem() != null ? ua.getOperatingSystem().getName() : null;
        String device = ua.getOperatingSystem() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;

        return new String[] { uaType, uaFamily, osName, device };
    }

    @Override
    public String getDisplayString(String[] strings) {
        return "agent_pars_udf(" + strings[0] + ")";
    }
}
