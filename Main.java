import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;

public class Main {
    public static S3Client s3;
    public static Ec2Client ec2;
    public static AmazonElasticMapReduce mapReduce;
    static String bucket = "dsps212ass2";

    public static void main(String[] args) {
        s3 = S3Client.builder().region(Region.US_WEST_2).build();
        ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();

        AWSCredentials credentials_profile = null;
        try {
            credentials_profile = new ProfileCredentialsProvider("default").getCredentials(); // specifies any named profile in .aws/credentials as the credentials provider
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load credentials from .aws/credentials file. " +
                            "Make sure that the credentials file exists and that the profile name is defined within it.",
                    e);
        }
        mapReduce = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials_profile))
                .withRegion(Regions.US_EAST_1)
                .build();

        HadoopJarStepConfig jarStep1 = new HadoopJarStepConfig()
                .withJar("s3://"+bucket+"/step1.jar")
                .withArgs("step1", "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data");
        StepConfig step1 = new StepConfig()
                .withName("step1")
                .withHadoopJarStep(jarStep1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep2 = new HadoopJarStepConfig()
                .withJar("s3://"+bucket+"/step2.jar")
                .withArgs("step2","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data");
        StepConfig step2 = new StepConfig()
                .withName("step2")
                .withHadoopJarStep(jarStep2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep3 = new HadoopJarStepConfig()
                .withJar("s3://"+bucket+"/step3.jar")
                .withArgs("step3","s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data");
        StepConfig step3 = new StepConfig()
                .withName("step3")
                .withHadoopJarStep(jarStep3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep4 = new HadoopJarStepConfig()
                .withJar("s3://"+bucket+"/step4.jar")
                .withArgs("step4");
        StepConfig step4 = new StepConfig()
                .withName("step4")
                .withHadoopJarStep(jarStep4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep5 = new HadoopJarStepConfig()
                .withJar("s3://"+bucket+"/step5.jar")
                .withArgs("step5");
        StepConfig step5 = new StepConfig()
                .withName("step5")
                .withHadoopJarStep(jarStep5)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        HadoopJarStepConfig jarStep6 = new HadoopJarStepConfig()
                .withJar("s3://"+bucket+"/step6.jar")
                .withArgs("step6","s3://"+bucket+"//outputAssignment2");
        StepConfig step6 = new StepConfig()
                .withName("step6")
                .withHadoopJarStep(jarStep6)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(3)
                .withMasterInstanceType(InstanceType.M3Xlarge.toString())
                .withSlaveInstanceType(InstanceType.M3Xlarge.toString())
                .withHadoopVersion("2.6.0")
                .withEc2KeyName("dsps221_1")
                .withKeepJobFlowAliveWhenNoSteps(false);

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Assignment2")
                .withReleaseLabel("emr-5.20.0")
                .withInstances(instances)
                .withSteps(step1, step2, step3, step4, step5, step6)
                .withLogUri("s3://"+bucket+"/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole");

        RunJobFlowResult result = mapReduce.runJobFlow(request);
        String jobFlowId = result.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}
