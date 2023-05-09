import com.alibaba.fastjson.JSON;

public class Demo {
    public static void main(String[] args) {
        String s = "{\"a\":\"{\"a1\":\"a2\"}\",\"b\":\"b1\"}";
        boolean valid = JSON.isValid(s);
        System.out.println(valid);
    }
}
