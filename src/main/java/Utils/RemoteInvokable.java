package Utils;

import java.util.Map;

public interface RemoteInvokable {

    void invoke();

    void invokeWithParameters(Map<String, Object> parameters);

}
