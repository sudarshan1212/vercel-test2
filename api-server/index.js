const express = require("express");
const { generateSlug } = require("random-word-slugs");
const { ECSClient, RunTaskCommand } = require("@aws-sdk/client-ecs");
const { Server } = require("socket.io");
const { z } = require("zod");
const { PrismaClient } = require("@prisma/client");
const { createClient } = require("@clickhouse/client");
const { Kafka } = require("kafkajs");
const { v4: uuidv4 } = require("uuid");
const fs = require("fs");
const path = require("path");

const app = express();
const PORT = 9000;
app.use(express.json());

const prisma = new PrismaClient({});
const kafka = new Kafka({
  clientId: `api-server`,
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

const client = createClient({
  host: "https://clickhouse-22cbadf9-homekraft12-207d.l.aivencloud.com:18886",
  database: "default",
  username: "avnadmin",
  password: "AVNS_0k8kHOwTtQuxGhcpIKz",
});

const consumer = kafka.consumer({ groupId: "api-server-logs-consumer" });
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
app.post("/project", async (req, res) => {
  const schema = z.object({
    name: z.string(),
    gitURL: z.string(),
  });
  const safeParseResult = schema.safeParse(req.body);
  if (safeParseResult.error)
    return res
      .status(400)
      .json({ Status: "INVALID", error: safeParseResult.error });
  const { name, gitURL } = safeParseResult.data;
  const project = await prisma.project.create({
    data: {
      name,
      gitURL,
      subDomain: generateSlug(),
    },
  });
  return res.status(200).json({ Status: "SUCCESS", data: { project } });
});

// this will work under the  ecs container service task what we are creating
app.use("/deploy", async (req, res) => {
  const { projectId } = req.body;
  const project = await prisma.project.findUnique({ where: { id: projectId } });
  if (!project) return res.status(404).json({ error: "Project Not Found" });
  const deployment = await prisma.deployement.create({
    data: {
      project: { connect: { id: projectId } },
      status: "QUEUED",
    },
  });
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
            { name: "GIT_REPOSITORY__URL", value: project.gitURL },
            {
              name: "PROJECT_ID",
              value: projectId,
            },
            {
              name: "DEPLOYEMENT_ID",
              value: deployment.id,
            },
          ],
        },
      ],
    },
  });
  await ecsClient.send(command);
  return res.json({
    Status: "queued",
    data: {
      deploymentId: deployment.id,
    },
  });
});

app.get("/logs/:id", async (req, res) => {
  const id = req.params.id;
  const logs = await client.query({
    query: `SELECT event_id, deployment_id, log, timestamp from log_events where deployment_id = {deployment_id:String}`,
    query_params: {
      deployment_id: id,
    },
    format: "JSONEachRow",
  });

  const rawLogs = await logs.json();

  return res.json({ logs: rawLogs });
});
async function initKafkConsumer() {
  await consumer.connect();
  await consumer.subscribe({ topics: ["container-logs"] });
  await consumer.run({
    autoCommit: false,
    eachBatch: async function ({
      batch,
      heartbeat,
      commitOffsetsIfNecessary,
      resolveOffset,
    }) {
      const messages = batch.messages;
      console.log("getting kafka");

      console.log(`Recv . ${messages.length} messages..`);
      for (const message of messages) {
        const stringMessage = message.value.toString();
        const { PROJECT_ID, DEPLOYEMENT_ID, log } = JSON.parse(stringMessage);
        console.log({ PROJECT_ID, DEPLOYEMENT_ID, log });

        try {
          const { query_id } = await client.insert({
            table: "log_events",
            values: [
              { event_id: uuidv4(), deployment_id: DEPLOYEMENT_ID, log },
            ],
            format: "JSONEachRow",
          });
          console.log(query_id);
          resolveOffset(message.offset);
          await commitOffsetsIfNecessary(message.offset);
          await heartbeat();
        } catch (error) {
          console.log("this is the error in kafks");

          console.log(err);
        }
      }
    },
  });
}
initKafkConsumer();
app.listen(PORT, () => {
  console.log(`Api Server is Running... ${PORT}`);
});
