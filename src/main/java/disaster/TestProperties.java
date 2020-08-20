package disaster;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.ResourceBundle;

public class TestProperties {
	public static void main(String[] args) throws IOException {
		readProperties1();
		readProperties2();
		readProperties3();
	}
	
	public static void readProperties1() throws IOException {

		//使用ClassLoader加载properties配置文件生成对应的输入流
		InputStream stream = TestProperties.class.getClassLoader().getResourceAsStream("avengers.properties");
		//使用properties对象加载输入流
		Properties prop = new Properties();
		prop.load(stream);
		//获取key对应的value值
		System.out.println(prop.getProperty("1"));
	}
	
	public static void readProperties2() throws IOException {
		
		String path = System.getProperty("user.dir")+"\\config\\avengers.properties";
		BufferedReader br = new BufferedReader(new FileReader(path));
		Properties prop = new Properties();
		prop.load(br);
		 
		System.out.println(prop.getProperty("2"));
	}
	
	public static void readProperties3() {
		ResourceBundle b = ResourceBundle.getBundle("avengers");
		System.out.println(b.getString("3"));
	}
}
