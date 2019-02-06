package io.faust.s3;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.*;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;


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

    @Parameter(property = "region", required = false)
    private String region;

    public void execute() throws MojoExecutionException {
        deploy();
    }


    private void deploy() throws MojoExecutionException {
        try {
            File fileToUpload = new File(path, file);
            AmazonS3 s3Client;
            if (region != null) {
                s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();
            } else {
                s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.DEFAULT_REGION).build();
            }
            String targetKey = new File(key, file).getPath();
            putEncryptedObject(fileToUpload, s3Client, targetKey);
            putObjectTagging(fileToUpload, s3Client, targetKey);

        } catch (Exception e) {
            getLog().error("could not upload file", e);
            throw new MojoExecutionException("could not upload file", e);
        }
    }

    private static String fileSha256ToBase64(File file) throws NoSuchAlgorithmException, IOException {
        byte[] data = Files.readAllBytes(file.toPath());
        MessageDigest digester = MessageDigest.getInstance("SHA-256");
        digester.update(data);
        return Base64.getEncoder().encodeToString(digester.digest());
    }

    private void putEncryptedObject(File fileToUpload, AmazonS3 s3Client, String target) throws FileNotFoundException {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(fileToUpload.length());
        objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

        InputStream targetStream = new FileInputStream(fileToUpload);
        s3Client.putObject(bucket, target, targetStream, objectMetadata);
    }

    private void putObjectTagging(File fileToUpload, AmazonS3 s3Client, String target) throws IOException, NoSuchAlgorithmException {
        Tag base64sha256tag = new Tag("base64sha256", fileSha256ToBase64(fileToUpload));
        List<Tag> tags = new ArrayList<>();
        tags.add(base64sha256tag);
        ObjectTagging objectTagging = new ObjectTagging(tags);
        SetObjectTaggingRequest setObjectTaggingRequest = new SetObjectTaggingRequest(bucket, target, objectTagging);
        s3Client.setObjectTagging(setObjectTaggingRequest);
    }

}
