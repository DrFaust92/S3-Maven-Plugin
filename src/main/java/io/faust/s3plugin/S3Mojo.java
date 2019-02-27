package io.faust.s3plugin;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.securitytoken.AWSSecurityTokenService;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetSessionTokenRequest;
import com.amazonaws.services.securitytoken.model.GetSessionTokenResult;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;


@Mojo(name = "s3-deploy",
        defaultPhase = LifecyclePhase.DEPLOY,
        requiresProject = true,
        threadSafe = true)
public class S3Mojo extends AbstractMojo {

    @Parameter(defaultValue = "${project.build.directory}",
            property = "sourcePath", required = false)
    private String path;

    @Parameter(property = "destination", required = true)
    private String key;

    @Parameter(property = "bucket", required = true)
    private String bucket;

    @Parameter(property = "file", required = true)
    private String file;

    @Parameter(property = "region", required = true)
    private String region;

    @Parameter(property = "role", required = false)
    private String role;

    private String roleSessionName = "S3MavenPluginSession";


    public void execute() throws MojoExecutionException {
        deploy();
    }


    private void deploy() throws MojoExecutionException {
        try {
            S3Utils s3Utils = new S3Utils();
            File fileToUpload = new File(path, file);
            AmazonS3ClientBuilder s3ClientawsClientBuilder;


            AWSStaticCredentialsProvider creds;

            if (role != null) {
                creds = new AWSStaticCredentialsProvider(assumeRole());
            } else {
                creds = null;
            }

            s3ClientawsClientBuilder = AmazonS3ClientBuilder.standard().withCredentials(creds).withRegion(region);

            AmazonS3 s3Client = s3ClientawsClientBuilder.build();
            String targetKey = new File(key, file).getPath();
            s3Utils.putEncryptedObject(bucket, fileToUpload, s3Client, targetKey);
            s3Utils.putObjectTagging(bucket, fileToUpload, s3Client, targetKey);

        } catch (Exception e) {
            getLog().error("could not upload file", e);
            throw new MojoExecutionException("could not upload file", e);
        }
    }


    private BasicSessionCredentials assumeRole() {
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
