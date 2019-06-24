package io.faust.s3plugin;

import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;

public class STSUtils {

    private String region;
    private String role;
    private String roleSessionName = "RoleSessionName1";

    public STSUtils(String region, String role) {
        this.region = region;
        this.role = role;
    }

    public BasicSessionCredentials assumeRole() {
        // Creating the STS client is part of your trusted code. It has
        // the security credentials you use to obtain temporary security credentials.
        AWSSecurityTokenService stsClient = AWSSecurityTokenServiceClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(region)
                .build();

        // Assume the IAM role. Note that you cannot assume the role of an AWS root account;
        // Amazon S3 will deny access. You must use credentials for an IAM user or an IAM role.
        AssumeRoleRequest roleRequest = new AssumeRoleRequest()
                .withRoleArn(role)
                .withRoleSessionName(roleSessionName);
        stsClient.assumeRole(roleRequest);

        // Start a session.
        GetSessionTokenRequest getSessionTokenRequest = new GetSessionTokenRequest();
        // The duration can be set to more than 3600 seconds only if temporary
        // credentials are requested by an IAM user rather than an account owner.
        getSessionTokenRequest.setDurationSeconds(7200);
        GetSessionTokenResult sessionTokenResult = stsClient.getSessionToken(getSessionTokenRequest);
        Credentials sessionCredentials = sessionTokenResult.getCredentials();

        // Package the temporary security credentials as a BasicSessionCredentials object
        // for an Amazon S3 client object to use.
        BasicSessionCredentials basicSessionCredentials = new BasicSessionCredentials(
                sessionCredentials.getAccessKeyId(), sessionCredentials.getSecretAccessKey(),
                sessionCredentials.getSessionToken());

        return basicSessionCredentials;
    }

}
