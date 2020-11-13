package kerberos.mykerberos;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedAction;

public class MyClient {

    public static void main(String[] args) {
        reloadProperty();
        String loginModuleName = "KerberosLogin";
        try {
            LoginContext context = new LoginContext(loginModuleName);

            context.login();
            System.out.println("pprincipal-->" + context.getSubject().getPrincipals());

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

    public static void reloadProperty() {
        System.setProperty("java.security.krb5.conf", "/Users/yanghuai/kerberos/local/krb5.conf");
        System.setProperty("java.security.auth.login.config", "/Users/yanghuai/kerberos/local/jaas2.conf");
    }
}
