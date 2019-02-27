package io.faust.s3plugin;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.ObjectTagging;
import com.amazonaws.services.s3.model.SetObjectTaggingRequest;
import com.amazonaws.services.s3.model.Tag;

import java.io.*;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

class S3Utils {


    static String fileSha256ToBase64(File file) throws NoSuchAlgorithmException, IOException {
        byte[] data = Files.readAllBytes(file.toPath());
        MessageDigest digester = MessageDigest.getInstance("SHA-256");
        digester.update(data);
        return Base64.getEncoder().encodeToString(digester.digest());
    }

    void putEncryptedObject(String bucketName, File fileToUpload, AmazonS3 s3Client, String target) throws FileNotFoundException {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentLength(fileToUpload.length());
        objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

        InputStream targetStream = new FileInputStream(fileToUpload);
        s3Client.putObject(bucketName, target, targetStream, objectMetadata);
    }

    void putObjectTagging(String bucketName, File fileToUpload, AmazonS3 s3Client, String target) throws IOException, NoSuchAlgorithmException {
        Tag base64sha256tag = new Tag("base64sha256", fileSha256ToBase64(fileToUpload));
        List<Tag> tags = new ArrayList<>();
        tags.add(base64sha256tag);
        ObjectTagging objectTagging = new ObjectTagging(tags);
        SetObjectTaggingRequest setObjectTaggingRequest = new SetObjectTaggingRequest(bucketName, target, objectTagging);
        s3Client.setObjectTagging(setObjectTaggingRequest);
    }
}
