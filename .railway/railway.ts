import { bucket, defineRailway, github, group, project, ref, service, volume } from "railway/iac";

// One Camus broker. Kafka is PLAINTEXT on the public TCP proxy. Admin HTTP
// listens on Railway's PORT and is not given a public domain.
export default defineRailway(() => {
  const logs = bucket("logs", { region: "iad" });
  const data = volume("camus-data", { sizeMB: 1024 });

  const broker = service("camus", {
    source: github("nidheesh-m-vakharia/camus"),
    build: { builder: "DOCKERFILE" },
    healthcheck: "/health",
    healthcheckTimeout: 60,
    tcp: [9092],
    volumeMounts: {
      "/data": data,
    },
    env: {
      // The image drops privileges. A Railway volume is root-owned.
      RAILWAY_RUN_UID: "0",
      // Stay under the 1 GiB volume. The default hold is 8 GiB.
      CAMUS_WAL_DISK_LIMIT: "268435456",
      CAMUS_S3_ENDPOINT: ref(logs, "ENDPOINT"),
      CAMUS_S3_BUCKET: ref(logs, "BUCKET"),
      CAMUS_S3_REGION: ref(logs, "REGION"),
      CAMUS_S3_ACCESS_KEY: ref(logs, "ACCESS_KEY_ID"),
      CAMUS_S3_SECRET_KEY: ref(logs, "SECRET_ACCESS_KEY"),
      CAMUS_S3_VIRTUAL_HOST: "1",
    },
  });

  return project("camus", {
    resources: [group("Camus", [broker, logs, data])],
  });
});
