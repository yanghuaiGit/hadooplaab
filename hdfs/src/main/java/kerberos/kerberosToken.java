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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import sun.security.krb5.Config;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

public class kerberosToken {

    public static void main(String[] args) throws Exception {

        reloadKrb5Conf();
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl.disable.cache", "true");
        conf.set("hadoop.security.authorization", "true");
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("fs.defaultFS", "hdfs://eng-cdh1:8020");
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@DTSTACK.COM");

        UserGroupInformation.setConfiguration(conf);

        UserGroupInformation tokenUGI = UserGroupInformation.createRemoteUser("yarn");

        UserGroupInformation kerberosUGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hdfs/eng-cdh1@DTSTACK.COM", "/Users/yanghuai/fsdownload/hdfs.keytab");
        Credentials creds = new Credentials();

        kerberosUGI.doAs((PrivilegedExceptionAction<Void>) () -> {
            FileSystem fs = FileSystem.get(conf);
            // get delegation tokens, add them to the provided credentials. set renewer to ‘yarn’
            Token<?>[] newTokens = fs.addDelegationTokens("hdfs", creds);
            // Add all the credentials to the UGI object
            tokenUGI.addCredentials(creds);
            // Alternatively, you can add the tokens to UGI one by one.
            // This is functionally the same as the above, but you have the option to log each token acquired.
            for (Token<?> token : newTokens) {
                tokenUGI.addToken(token);
            }

            return null;
        });


        tokenUGI.doAs((PrivilegedExceptionAction<Void>) () -> {

            FileSystem fileSystem = FileSystem.get(conf);
            Path path = new Path("/data/sftp_38/confPath/CONSOLE_kerberos_v1/yarn-site.xml");
            FSDataInputStream fsDataInputStream = fileSystem.open(path);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            IOUtils.copyBytes(fsDataInputStream, stream, 1024);
            System.out.println(new String(stream.toByteArray()));
            FileStatus fileStatus = fileSystem.getFileStatus(new Path("/data/sftp_38/confPath/CONSOLE_kerberos_v1/yarn-site.xml"));// ← tokenUGI can authenticate via Delegation Token
            System.out.println(fileStatus);
            return null;
        });


    }


    private static void reloadKrb5Conf() {


        System.setProperty("java.security.krb5.conf", "/Users/yanghuai/Downloads/hbase_kerberos_108/krb5.conf");

        try {
            if (!System.getProperty("java.vendor").contains("IBM")) {
                Config.refresh();
            }
        } catch (Exception e) {

        }
    }
}
