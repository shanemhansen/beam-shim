package ai.shane.bigtableshim;
import java.util.Map;
import java.util.Set;

public class ConfigMap {
    public Map<String, String> values;
    ConfigMap() {}
    public void setValues(Map<String, String> v) {
        values = v;
    }
    ConfigMap(Map<String, String> entries) {
        this.values = entries;
        values.entrySet();
    }
    public Set<Map.Entry<String, String>> entrySet() {
        return values.entrySet();
    }
}
