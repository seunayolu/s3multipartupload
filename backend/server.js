import express from 'express';
import multer from 'multer';
import cors from 'cors';
import { SSMClient, GetParametersCommand } from "@aws-sdk/client-ssm";
import { 
  S3Client, 
  CreateMultipartUploadCommand, 
  UploadPartCommand, 
  CompleteMultipartUploadCommand, 
  AbortMultipartUploadCommand 
} from "@aws-sdk/client-s3";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";

const app = express();
app.use(cors());
app.use(express.json());

const upload = multer({ storage: multer.memoryStorage() });
const region = process.env.AWS_REGION || "us-east-1";

const ssm = new SSMClient({ region });
let s3, dynamo, config = {};

app.get('/health', (req, res) => res.status(200).send('OK'));

async function bootstrap() {
  try {
    const { Parameters } = await ssm.send(new GetParametersCommand({
      Names: ["/app/s3-bucket", "/app/dynamo-table"],
      WithDecryption: true
    }));
    
    Parameters.forEach(p => {
      const key = p.Name.split('/').pop().replace('-', '');
      config[key] = p.Value;
    });

    s3 = new S3Client({ region });
    dynamo = new DynamoDBClient({ region });
    
    app.listen(3000, () => console.log("🚀 Manual Multipart Backend active on port 3000"));
  } catch (err) {
    console.error("Bootstrap Error:", err);
    process.exit(1);
  }
}

app.post('/upload', upload.single('file'), async (req, res) => {
  const metadata = JSON.parse(req.body.metadata);
  const file = req.file;
  const key = `${metadata.category}/${Date.now()}-${file.originalname}`;
  let uploadId;

  try {
    // --- PHASE 1: INITIATE ---
    const init = await s3.send(new CreateMultipartUploadCommand({
      Bucket: config.s3bucket,
      Key: key,
      ContentType: file.mimetype
    }));
    uploadId = init.UploadId;
    console.log(`[S3] Handshake Started. UploadId: ${uploadId}`);

    const CHUNK_SIZE = 1024 * 1024 * 5; // 5MB
    const totalParts = Math.ceil(file.size / CHUNK_SIZE);
    const completedParts = [];

    // --- PHASE 2: CHUNKING ---
    for (let i = 0; i < totalParts; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, file.size);
      const partNum = i + 1;

      const partResponse = await s3.send(new UploadPartCommand({
        Bucket: config.s3bucket,
        Key: key,
        UploadId: uploadId,
        PartNumber: partNum,
        Body: file.buffer.subarray(start, end)
      }));

      console.log(`[S3] Part ${partNum}/${totalParts} uploaded. ETag: ${partResponse.ETag}`);
      completedParts.push({ ETag: partResponse.ETag, PartNumber: partNum });
    }

    // --- PHASE 3: FINALIZE ---
    const finalResult = await s3.send(new CompleteMultipartUploadCommand({
      Bucket: config.s3bucket,
      Key: key,
      UploadId: uploadId,
      MultipartUpload: { Parts: completedParts }
    }));

    // Construct the file URL
    const fileUrl = `https://${config.s3bucket}.s3.${region}.amazonaws.com/${key}`;
    console.log(`[S3] Finished. Final ETag: ${finalResult.ETag}`);

    // --- PHASE 4: PERSIST ---
    await dynamo.send(new PutItemCommand({
      TableName: config.dynamotable,
      Item: {
        "FileID": { S: Date.now().toString() },
        "Title": { S: metadata.title },
        "Category": { S: metadata.category },
        "S3_Url": { S: fileUrl },
        "S3_ETag": { S: finalResult.ETag },
        "FileSize_MB": { N: (file.size / (1024 * 1024)).toFixed(2) }
      }
    }));

    res.status(200).json({ status: "Success", url: fileUrl });

  } catch (err) {
    console.error("[S3 ERROR]", err);
    if (uploadId) {
      await s3.send(new AbortMultipartUploadCommand({ 
        Bucket: config.s3bucket, Key: key, UploadId: uploadId 
      }));
    }
    res.status(500).json({ error: err.message });
  }
});

bootstrap();