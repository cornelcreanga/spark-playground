/*
 * Checkpoint File Manager for MinIO (C) 2023 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.creanga.playground.spark;

import com.amazonaws.auth.*;
import org.apache.hadoop.conf.Configuration;

public class Credentials {


    private static AWSCredentialsProvider staticCredentialsProvider(AWSCredentials credentials){
        return new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
                return credentials;
            }

            @Override
            public void refresh() {

            }
        };
    }

    public static AWSCredentialsProvider load (Configuration hadoopConfiguration) {
        String accessKey = hadoopConfiguration.get("fs.s3a.access.key", null);
        String secretKey = hadoopConfiguration.get("fs.s3a.secret.key", null);
        String sessionToken = hadoopConfiguration.get("fs.s3a.session.token", null);

        if (accessKey != null && secretKey != null) {
            if (sessionToken != null) {
                return staticCredentialsProvider(new BasicSessionCredentials(accessKey, secretKey, sessionToken));
            }else{
                return staticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
            }
        }else{
            return new DefaultAWSCredentialsProviderChain();
        }
    }
}