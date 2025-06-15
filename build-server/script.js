const { exec } = require("child_process");
const path = require("path");
const fs = require("fs");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const mime = require("mime-types");
const Redis = require("ioredis");
const publisher = new Redis(
  "rediss://default:AVNS_ESFVj3RXBMgsoqcUs9w@redis-188ff3e9-homekraft12-207d.b.aivencloud.com:18886"
);
const s3 = new S3Client({
  region: "eu-north-1",
  credentials: {
    accessKeyId: "AKIA4IM3HFV7JZ7RXB6R",
    secretAccessKey: "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",
  },
});
const PROJECT_ID = process.env.PROJECT_ID;
function publishLog(log) {
  publisher.publish(`logs:${PROJECT_ID}`, JSON.stringify({ log }));
}
async function init() {
  console.log("Script is Running");
  publishLog("Build Started ....");
  const outDirPath = path.join(__dirname, "output");
  // /home/app/output this is outDirPath

  const p = exec(`cd ${outDirPath} && npm install && npm run build`); //* /home/app
  // it will log what this code doing
  p.stdout.on("data", function (data) {
    console.log(data.toString());
    publishLog(data.toString());
  });

  p.on("error", function (err) {
    console.log("Error", err.toString());
    publishLog(`error: ${err.toString()}`);
  });
  p.on("close", async function () {
    console.log("Build Complete");
    publishLog(`Build Complete`);
    const distFolderPath = path.join(__dirname, "output", "dist");
    // /home/app/output/dist this is the distFolderPath
    // GETTING ALL THE FILES FROM THE DIST
    const distFolderContent = fs.readdirSync(distFolderPath, {
      recursive: true,
    });
    console.log(distFolderContent, "this is the distFolderContent");

    publishLog(`Starting to upload`);
    for (const file of distFolderContent) {
      const filePath = path.join(distFolderPath, file);
      //THIS IS FOR UPLOADIN ONLY THE FILEES NOT FOLDERS AND HERE WE ARE NOT UPLAODING THE ASSETS WE UPLOADING LIE THIS assets/main.js
      if (fs.lstatSync(filePath).isDirectory()) continue;
      // Uploading.... /home/app/output/dist/index.html
      console.log("Uploading....", filePath);
      publishLog(`uploading ${file}`);

      const command = new PutObjectCommand({
        Bucket: "vercel-clone-sudarshan",
        Key: `__outputs/${PROJECT_ID}/${file}`,
        Body: fs.createReadStream(filePath),
        ContentType: mime.lookup(filePath),
      });
      await s3.send(command);
      publishLog(`uploaded ${file}`);
      // Uploaded.... /home/app/output/dist/assets/index-DK-xQhXp.js
      console.log("Uploaded....", filePath);
    }
    publishLog(`Done`);
    console.log("Done...");
  });
}
init();
