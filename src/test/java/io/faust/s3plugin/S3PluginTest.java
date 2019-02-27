package io.faust.s3plugin;


import com.adobe.testing.s3mock.junit4.S3MockRule;
import com.adobe.testing.s3mock.util.HashUtil;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class S3PluginTest {

    @ClassRule
    public static final S3MockRule S3_MOCK_RULE = S3MockRule.builder().silent().build();

    private static final String BUCKET_NAME = "mydemotestbucket";
    private static final String UPLOAD_FILE_NAME = "src/test/resources/sampleFile.txt";
    private final AmazonS3 s3Client = S3_MOCK_RULE.createS3Client();
    private final S3Utils s3Utils = new S3Utils();


    @Test
    public void shouldUploadAndDownloadObject() throws Exception {
        s3Client.createBucket(BUCKET_NAME);

        final File uploadFile = new File(UPLOAD_FILE_NAME);
        String targetKey = new File("target", UPLOAD_FILE_NAME).getPath();
        s3Utils.putEncryptedObject(BUCKET_NAME, uploadFile, s3Client, targetKey);


        final S3Object s3Object = s3Client.getObject(BUCKET_NAME, targetKey);

        final InputStream uploadFileIs = new FileInputStream(uploadFile);
        final String uploadHash = HashUtil.getDigest(uploadFileIs);
        final String downloadedHash = HashUtil.getDigest(s3Object.getObjectContent());
        uploadFileIs.close();
        s3Object.close();

        assertThat("Up- and downloaded Files should have equal Hashes", uploadHash,
                is(equalTo(downloadedHash)));
    }


    @Test
    public void shouldTagObject() throws Exception {
        s3Client.createBucket(BUCKET_NAME);

        final File uploadFile = new File(UPLOAD_FILE_NAME);
        String targetKey = new File("target", UPLOAD_FILE_NAME).getPath();
        s3Utils.putEncryptedObject(BUCKET_NAME, uploadFile, s3Client, targetKey);
        s3Utils.putObjectTagging(BUCKET_NAME, uploadFile, s3Client, targetKey);


        GetObjectTaggingRequest getObjectTaggingRequest = new GetObjectTaggingRequest(BUCKET_NAME, targetKey);
        final GetObjectTaggingResult s3ObjectTag = s3Client.getObjectTagging(getObjectTaggingRequest);

        String uploadedFileTagValue = S3Utils.fileSha256ToBase64(uploadFile);
        String downloadedFileTagValue = s3ObjectTag.getTagSet().get(0).getValue();

        assertThat("Uploaded File should have a base64sha256 tag",
                uploadedFileTagValue, is(equalTo(downloadedFileTagValue)));
    }

}
