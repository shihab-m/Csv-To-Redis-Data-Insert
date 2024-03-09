const csv = require("csv-parser");
const fs = require("fs");
const Redis = require("ioredis");

const redisOptions = {
  host: "localhost",
  port: 6379,
};

const batchSize = 1000;
const redis = new Redis(redisOptions);

let totalInserted = 0;

const insertData = async (data) => {
  const pipeline = redis.pipeline();

  for (const row of data) {
    const key = row.uid;
    const value = row.shard;

    pipeline.set(key, value);
    totalInserted++;
  }

  await pipeline.exec();
};

const setPromises = [];

const readStream = fs.createReadStream("./your-file.csv").pipe(csv());

readStream
  .on("data", (data) => {
    if (setPromises.length >= batchSize) {
      const dataBatch = setPromises.splice(0, setPromises.length);
      setPromises.push(insertData(dataBatch));
    }

    const key = data.uid;
    const value = data.shard;

    setPromises.push({ key, value });
  })
  .on("end", async () => {
    if (setPromises.length > 0) {
      await insertData(setPromises);
    }

    console.log(`Inserted ${totalInserted} records into Redis.`);
    redis.disconnect();
  })
  .on("error", (error) => {
    console.error(`Error reading CSV file: ${error}`);
    redis.disconnect();
  });
