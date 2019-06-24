package io.faust.s3plugin;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
            STSUtils stsUtils = new STSUtils(region, role);


            AWSStaticCredentialsProvider creds;

            if (role != null) {
                creds = new AWSStaticCredentialsProvider(stsUtils.assumeRole());
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

}
