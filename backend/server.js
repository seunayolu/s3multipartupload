import express from 'express';
import multer from 'multer';
import cors from 'cors';
import { SSMClient, GetParametersCommand } from "@aws-sdk/client-ssm";
import { S3Client } from "@aws-sdk/client-s3";
import { DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import { Upload } from "@aws-sdk/lib-storage";

const app = express();
app.use(cors());
app.use(express.json());

// Memory storage is used so we don't fill up the EC2 disk with temporary 1GB files
const upload = multer({ storage: multer.memoryStorage() });

const ssm = new SSMClient();
let s3, dynamo, config = {};

/**
 * PHASE 1: BOOTSTRAP
 * Fetches Bucket and Table names from SSM at startup.
 */
async function bootstrap() {
  console.log("[STARTUP] Initializing: Fetching configuration from SSM...");
  
  try {
    const command = new GetParametersCommand({
      Names: ["/app/s3-bucket", "/app/dynamo-table"],
      WithDecryption: true
    });

    const { Parameters } = await ssm.send(command);
    
    // Map parameters to our config object for easy access
    Parameters.forEach(p => {
      const key = p.Name.split('/').pop().replace('-', '');
      config[key] = p.Value;
    });

    // Verify we got what we needed
    if (!config.s3bucket || !config.dynamotable) {
      throw new Error("Missing required SSM parameters: /app/s3-bucket or /app/dynamo-table");
    }

    s3 = new S3Client();
    dynamo = new DynamoDBClient();

    console.log(`[STARTUP] Success: Using Bucket [${config.s3bucket}] and Table [${config.dynamotable}]`);
    
    app.listen(3000, () => console.log("Backend active on port 3000"));
  } catch (err) {
    console.error("[FATAL ERROR] Bootstrap failed. The application will now exit.", err.message);
    process.exit(1); 
  }
}

// Health Check Endpoint
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

/**
 * PHASE 2: THE UPLOAD HANDLER
 * Handles the file stream and the subsequent database entry.
 */
app.post('/upload', upload.single('file'), async (req, res) => {
  const metadata = JSON.parse(req.body.metadata);
  const file = req.file;

  if (!file) {
    return res.status(400).send("No file received.");
  }

  try {
    console.log(`[LOG] Starting Multi-part upload for ${metadata.category}: "${metadata.title}"`);

    // 1. Stream to S3
    const task = new Upload({
      client: s3,
      params: {
        Bucket: config.s3bucket,
        Key: `${metadata.category}/${Date.now()}-${file.originalname}`,
        Body: file.buffer,
        ContentType: file.mimetype
      },
      partSize: 1024 * 1024 * 5, // 5MB Chunks
      queueSize: 3 // How many chunks to upload concurrently
    });

    task.on("httpUploadProgress", (progress) => {
      // These logs will show up in CloudWatch via the EC2 console/logs
      const percent = Math.round((progress.loaded / progress.total) * 100);
      console.log(`[PROGRESS] Uploading ${metadata.title}: ${percent}%`);
    });

    await task.done();
    console.log(`[SUCCESS] File stored in S3 bucket: ${config.s3bucket}`);

    // 2. Save Metadata to DynamoDB
    const dbItem = {
      TableName: config.dynamotable,
      Item: {
        "FileID": { S: Date.now().toString() },
        "Category": { S: metadata.category },
        "Title": { S: metadata.title },
        "Year": { N: metadata.year.toString() },
        "Description": { S: metadata.description || "N/A" },
        "Author": metadata.details.author ? { S: metadata.details.author } : { NULL: true },
        "ISBN": metadata.details.isbn ? { S: metadata.details.isbn } : { NULL: true },
        "Artist": metadata.details.artist ? { S: metadata.details.artist } : { NULL: true },
        "Resolution": metadata.details.resolution ? { S: metadata.details.resolution } : { NULL: true }
      }
    };

    await dynamo.send(new PutItemCommand(dbItem));
    console.log(`[SUCCESS] Metadata indexed in DynamoDB: ${config.dynamotable}`);

    res.status(200).json({ status: "Complete", message: "Successfully uploaded and indexed." });

  } catch (err) {
    console.error("[UPLOAD ERROR]", err);
    res.status(500).json({ status: "Error", message: err.message });
  }
});

bootstrap();