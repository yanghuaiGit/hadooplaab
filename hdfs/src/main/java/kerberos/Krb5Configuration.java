package kerberos;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

public class Krb5Configuration extends Configuration {
    private AppConfigurationEntry[] entry = new AppConfigurationEntry[1];

    Map paramMap = new HashMap();

    private AppConfigurationEntry krb5LoginModule = new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, paramMap);

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (entry[0] == null) {
            paramMap.put("debug", "true");

            //1.enter the username and passsword
            //paramMap.put("storeKey", "true");
            //paramMap.put("doNotPrompt", "false");

            //2.use keytab file
            paramMap.put("doNotPrompt", "true");
            paramMap.put("useKeyTab", "true");
            paramMap.put("keyTab", "/Users/yanghuai/kerberos/local/hdfsone.keytab");
            paramMap.put("principal", "hdfs/kbc@DTSTACK.COM");

            paramMap.put("useTicketCache", "true");
            paramMap.put("ticketCache", "/Users/yanghuai/Library/Application Support/JetBrains/IntelliJIdea2020.1/scratches");


            entry[0] = krb5LoginModule;
        }
        return entry;
    }


}
