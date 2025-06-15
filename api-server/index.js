const express = require("express");
const { generateSlug } = require("random-word-slugs");
const { ECSClient, RunTaskCommand } = require("@aws-sdk/client-ecs");
const { Server } = require("socket.io");
const Redis = require("ioredis");
const { z } = require("zod");
const { PrismaClient } = require("@prisma/client");
const { connect } = require("http2");
const app = express();
const PORT = 9000;
app.use(express.json());
const subscriber = new Redis(
  "rediss://default:AVNS_ESFVj3RXBMgsoqcUs9w@redis-188ff3e9-homekraft12-207d.b.aivencloud.com:18886"
);
const prisma = new PrismaClient({});
const io = new Server({ cors: "*" });
io.on("connection", (socket) => {
  socket.on("subscribe", (channel) => {
    socket.join(channel);
    socket.emit("message", `Joined ${channel}`);
  });
});
io.listen(9002, () => console.log("Socket Server 9002"));
// this is for iam
const ecsClient = new ECSClient({
  region: "us-east-1", // change to your region
  credentials: {
    accessKeyId: "AKIA4IM3HFV7JZ7RXB6R",
    secretAccessKey: "It2KTjrxDUk+PuL22VHy+BRgO349dyC1iUbDMuPb",
  },
});

// cluster created using
const config = {
  CLUSTER: "arn:aws:ecs:us-east-1:842675989886:cluster/builder-cluster",
  TASK: "arn:aws:ecs:us-east-1:842675989886:task-definition/builder-task:1",
};

// this will work under the  ecs container service task what we are creating
app.use("/deploy", async (req, res) => {
  const { gitUrl, slug } = req.body;
  const projectSlug = slug ? slug : generateSlug();
  const command = new RunTaskCommand({
    cluster: config.CLUSTER,
    taskDefinition: config.TASK,
    launchType: "FARGATE",
    count: 1,
    networkConfiguration: {
      awsvpcConfiguration: {
        assignPublicIp: "ENABLED",
        subnets: [
          "subnet-02ea77efd15f7de9f",
          "subnet-00a9e49d523acec3c",
          "subnet-0a96514d8746bd294",
          "subnet-01d2d34efd1ca8203",
          "subnet-0fc0722ff614cac2b",
          "subnet-09c651d6b29b0ad82",
        ],
        securityGroups: ["sg-072a5763c3d0dfed2"],
      },
    },
    overrides: {
      containerOverrides: [
        {
          name: "builder-image",
          environment: [
            { name: "GIT_REPOSITORY__URL", value: gitUrl },
            {
              name: "PROJECT_ID",
              value: projectSlug,
            },
          ],
        },
      ],
    },
  });
  await ecsClient.send(command);
  return res.json({
    Status: "queued",
    data: { projectSlug, url: `http://${projectSlug}.localhost:8000` },
  });
});
app.get("/", (req, res) => {
  res.status(200).json({ Status: "SUCCESS", message: "" });
});

function initRedisSubscribe() {
  console.log(`Subscribed to Logs....`);

  /**
   * Subscribes to all Redis channels that start with logs:
   * When any message is published on those channels...
   * It forwards that message in real-time to all WebSocket clients connected to the same channel using Socket.IO.
   */
  subscriber.psubscribe("logs:*"); // getting the message of the redis publisher
  //Redis publishes on logs:*	Your server receives it via subscriber.on("pmessage")
  subscriber.on("pmessage", (pattern, channel, message) => {
    io.to(channel).emit("message", message);
  }); //recivies the message
}
initRedisSubscribe();
app.listen(PORT, () => {
  console.log(`Api Server is Running... ${PORT}`);
});
