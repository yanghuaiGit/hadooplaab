/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kerberos;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;

public class PasswordDemo {
    private static String username = "test2/kbc";
    private static String passwd = "123";

    public static void main(String[] args) throws Exception {

        loadProperty();

        Krb5Configuration conf = new Krb5Configuration();
        try {
            LoginContext lc = new LoginContext("JaaSSampleTest",
                    new Subject(),
                    PasswordDemo.createJaasCallbackHandler(username,
                            passwd), conf);
            lc.login();
            Subject sub = lc.getSubject();
            System.out.println("sub-->"+sub);
        } catch (LoginException le) {
            System.exit(-1);
        }
        System.out.println("Authentication succeeded!");
    }

    public static CallbackHandler createJaasCallbackHandler(
            final String principal, final String password) {
        return new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException,
                    UnsupportedCallbackException {
                for (Callback callback : callbacks) {
                    if (callback instanceof NameCallback) {
                        NameCallback nameCallback = (NameCallback) callback;
                        nameCallback.setName(principal);
                    } else if (callback instanceof PasswordCallback) {
                        PasswordCallback passwordCallback = (PasswordCallback) callback;
                        passwordCallback.setPassword(password.toCharArray());
                    } else {
                        throw new UnsupportedCallbackException(callback,
                                "Unsupported callback: "
                                        + callback.getClass()
                                        .getCanonicalName());
                    }
                }
            }
        };
    }

    public static void loadProperty() {
        System.setProperty("java.security.krb5.conf", "/Users/yanghuai/kerberos/local/krb5.conf");
        System.setProperty("java.security.krb5.kdc", "kbs");
        System.setProperty("java.security.krb5.realm", "DTSTACK.COM");
    }
}
