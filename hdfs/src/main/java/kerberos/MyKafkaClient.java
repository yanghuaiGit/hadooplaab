package kerberos;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.security.PrivilegedAction;

public class MyKafkaClient {

    public static void main(String[] args) throws Exception {
        System.setProperty("java.security.krb5.conf", "/Users/yanghuai/kerberos/local/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Users/yanghuai/kerberos/local/jaas1.conf");
        String loginModuleName = "KerberosLogin";
        Krb5Configuration conf = new Krb5Configuration();
        try {
            String sep = File.separator;
            System.out.println("KerberosClient.main():"
                    + System.getProperty("java.home") + sep + "lib" + sep
                    + "security" + sep + "java.security");

            //登录的主体是java.security.auth.login.config配置里查找到KafkaClient里的principal
            LoginContext context = new LoginContext("test");
            //权限认证
            //login 成功 会对一个新的Subjec对象填入验证信息
            context.login();
            System.out.println(context.getSubject().getPrincipals());

            //用户的业务逻辑 使用login()方法返回的Subject对象实现一些特殊功能，假设登录成功
            //java的权限认证就和用户的业务逻辑分离了
            Subject subject = context.getSubject();

            Subject.doAs(subject, new PrivilegedAction() {
                @Override
                public Object run() {
                    System.out.println(context.getSubject());
                    return null;
                }
            });


        } catch (LoginException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
