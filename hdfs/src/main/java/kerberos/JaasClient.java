package kerberos;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedAction;

public class JaasClient {
    private static String loginModuleName = "KerberosLogin";

    public static void main(String[] args) throws Exception {
        reloadProperty();
        try {
            //登录的主体是conf的配置
//            LoginContext context = new LoginContext(loginModuleName);
            LoginContext context = new LoginContext(loginModuleName,null,null,new Krb5Configuration());

            //login 成功 会对一个新的Subjec对象填入验证信息
            context.login();
            System.out.println("principals--->"+context.getSubject().getPrincipals());

            Subject subject = context.getSubject();
            Subject.doAs(subject, new PrivilegedAction() {
                @Override
                public Object run() {
                    System.out.println("subject-->"+context.getSubject());
                    return null;
                }
            });
        } catch (LoginException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

//
    public static void reloadProperty() {
        System.setProperty("java.security.krb5.conf", "/Users/yanghuai/kerberos/local/krb5.conf");
        //配置文件 javax.security.auth.login.Configuration /java.security.auth.login.config
//        System.setProperty("java.security.auth.login.config", "/Users/yanghuai/kerberos/local/jaas1.conf");
    }

}
