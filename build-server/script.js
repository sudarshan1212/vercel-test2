const { exec } = require("child_process");
const path = require("path");
const fs = require("fs");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const mime = require("mime-types");
// const Redis = require("ioredis");
const { Kafka } = require("kafkajs");

// const publisher = new Redis(
//   "rediss://default:AVNS_ESFVj3RXBMgsoqcUs9w@redis-188ff3e9-homekraft12-207d.b.aivencloud.com:18886"
// );
const s3 = new S3Client({
  region: process.env.AWS_S3_REGION,
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
});
const PROJECT_ID = process.env.PROJECT_ID;
const DEPLOYEMENT_ID = process.env.DEPLOYEMENT_ID;
const kafka = new Kafka({
  clientId: `docker-build-server-${DEPLOYEMENT_ID}`,
  brokers: ["kafka-387fbee4-homekraft12-207d.b.aivencloud.com:18898"],
  ssl: {
    ca: [fs.readFileSync(path.join(__dirname, "kafka.pem"), "utf-8")],
  },
  sasl: {
    username: "avnadmin",
    password: "AVNS_i79Ds-vDBGsV810htHe",
    mechanism: "plain",
  },
});
// const kafka = new Kafka({
//   clientId: `docker-build-server-${process.env.DEPLOYEMENT_ID || "local"}`,
//   brokers: [process.env.KAFKA_BROKER],
//   ssl: {
//     ca: [fs.readFileSync(path.join(__dirname, process.env.KAFKA_CA_FILE), "utf-8")],
//   },
//   sasl: {
//     username: process.env.KAFKA_USERNAME,
//     password: process.env.KAFKA_PASSWORD,
//     mechanism: "plain",
//   },
// });
const producer = kafka.producer();
async function publishLog(log) {
  await producer.send({
    topic: `container-logs`,
    messages: [
      {
        key: "log",
        value: JSON.stringify({ PROJECT_ID, DEPLOYEMENT_ID, log }),
      },
    ],
  });
  // publisher.publish(`logs:${PROJECT_ID}`, JSON.stringify({ log }));
}
async function init() {
  await producer.connect();
  console.log("Script is Running");
  await publishLog("Build Started ....");
  const outDirPath = path.join(__dirname, "output");
  // /home/app/output this is outDirPath

  const p = exec(`cd ${outDirPath} && npm install && npm run build`); //* /home/app
  // it will log what this code doing
  p.stdout.on("data", function (data) {
    console.log(data.toString());
    publishLog(data.toString());
  });

  p.on("error", async function (err) {
    console.log("Error", err.toString());
    await publishLog(`error: ${err.toString()}`);
  });
  p.on("close", async function () {
    console.log("Build Complete");
    await publishLog(`Build Complete`);
    const distFolderPath = path.join(__dirname, "output", "dist");
    // /home/app/output/dist this is the distFolderPath
    // GETTING ALL THE FILES FROM THE DIST
    const distFolderContent = fs.readdirSync(distFolderPath, {
      recursive: true,
    });
    console.log(distFolderContent, "this is the distFolderContent");

    await publishLog(`Starting to upload`);
    for (const file of distFolderContent) {
      const filePath = path.join(distFolderPath, file);
      //THIS IS FOR UPLOADIN ONLY THE FILEES NOT FOLDERS AND HERE WE ARE NOT UPLAODING THE ASSETS WE UPLOADING LIE THIS assets/main.js
      if (fs.lstatSync(filePath).isDirectory()) continue;
      // Uploading.... /home/app/output/dist/index.html
      console.log("Uploading....", filePath);
      await publishLog(`uploading ${file}`);

      const command = new PutObjectCommand({
        Bucket: "vercel-clone-sudarshan",
        Key: `__outputs/${PROJECT_ID}/${file}`,
        Body: fs.createReadStream(filePath),
        ContentType: mime.lookup(filePath),
      });
      await s3.send(command);
      await publishLog(`uploaded ${file}`);
      // Uploaded.... /home/app/output/dist/assets/index-DK-xQhXp.js
      console.log("Uploaded....", filePath);
    }
    await publishLog(`Done`);
    console.log("Done...");
    process.exit(0);
  });
}
init();
