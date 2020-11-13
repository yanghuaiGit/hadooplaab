package kerberos.mykerberos;

import sun.security.krb5.Credentials;
import sun.security.krb5.EncryptionKey;
import sun.security.krb5.PrincipalName;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.kerberos.KerberosKey;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.kerberos.KeyTab;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.util.Map;

public class MyLoginModule implements LoginModule {
    // initial state
    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map<String, Object> sharedState;
    private Map<String, ?> options;

    private KerberosPrincipal kerbClientPrinc = null;


    //LoginContext在调用login（）方法时会调用initialize（）方法
    //CallbackHandler对象将会在login（）方法中被使用到。sharedState可以使数据在不同的LoginModule对象之间共享
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        System.out.println("loginModule initialize");
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.options = options;
        kerbClientPrinc = new KerberosPrincipal("hah");
        System.out.println(options);

    }

    @Override
    public boolean login() throws LoginException {
        System.out.println("loginModule login");
        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        subject.getPrincipals().add(kerbClientPrinc);
        subject.getPrivateCredentials().add("0---");
        System.out.println("loginModule commit");
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        System.out.println("loginModule abort");
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        System.out.println("loginModule logout");
        return false;
    }
}
