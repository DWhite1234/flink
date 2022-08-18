import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;

public class Test1 {
    public static void main(String[] args) throws IOException {
        InputStream resourceAsStream = Test1.class.getClassLoader().getResourceAsStream("test/druid.properties");
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(resourceAsStream);

        ParameterTool fromArgs = ParameterTool.fromArgs(args);

//        parameterTool = parameterTool.mergeWith(fromArgs);

        fromArgs=fromArgs.mergeWith(parameterTool);
//        System.out.println(parameterTool.toMap());
        System.out.println(fromArgs.toMap());
    }
}
