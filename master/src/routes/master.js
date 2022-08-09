import express, { response } from "express";
import {
  PRESHARED_MASTER_KEY
} from "../config";
const storage = require('node-persist');
const masterRouter = express.Router();
const client = require('prom-client');
const register = new client.Registry();
const collectDefaultMetrics = client.collectDefaultMetrics;
collectDefaultMetrics({ timeout: 3000 });
const promLatestLatency = new client.Gauge({
  name: 'orch_latest_latency',
  help: 'Latest latency known for a given Orchestrator',
  labelNames: ['region', 'orchestrator']
});
register.registerMetric(promLatestLatency);
const promLatency = new client.Summary({
  name: 'orch_latency',
  help: 'Summary of latency stats',
  percentiles: [0.01, 0.1, 0.9, 0.99],
  labelNames: ['region']
});
register.registerMetric(promLatency);
const promAverageLatency = new client.Gauge({
  name: 'orch_average_latency',
  help: 'Average latency for a given Orchestrator',
  labelNames: ['region', 'orchestrator']
});
register.registerMetric(promAverageLatency);
const promAUptimeScore = new client.Gauge({
  name: 'orch_uptime_score',
  help: 'Uptime score for a given orchestrator',
  labelNames: ['region', 'orchestrator']
});
register.registerMetric(promAUptimeScore);

let isSynced = false;

/*

Incoming stats parsing

*/

masterRouter.post("/collectStats", async (req, res) => {
  try {
    if (!isSynced){ console.log ("waiting for sync"); res.end('busy'); return;}
    const { id, discoveryResults,
      responseTime, tag, key } = req.body;
    if (!id || !tag || !key) {
      console.log("Received malformed data. Aborting stats update...");
      console.log(id, discoveryResults, responseTime, tag, key);
      res.send(false);
      return;
    }
    if (PRESHARED_MASTER_KEY != key) {
      console.log("Unauthorized");
      res.send(false);
      return;
    }
    console.log('received data for ' + id + ' from ' + tag + ' (' + responseTime + " ms latency)");
    if (responseTime){
      promLatestLatency.set({ region: tag, orchestrator: id }, responseTime);
      promLatency.observe({ region: tag }, responseTime);
    }
    // Save data point
    const now = new Date().getTime();
    let thisPing = responseTime;
    if (!discoveryResults || !responseTime) { thisPing = null; }
    let currentDataList = [];
    let orchFound = false;
    let regionFound = false;
    for (var orchIdx = 0; orchIdx < orchScores.length; orchIdx++) {
      if (orchScores[orchIdx].id != id) { continue; }
      orchFound = true;
      for (var regionIdx = 0; regionIdx < orchScores[orchIdx].data.length; regionIdx++) {
        if (orchScores[orchIdx].data[regionIdx].tag != tag) { continue; }
        regionFound = true;
        if (orchScores[orchIdx].data[regionIdx].data.length > 60) {
          orchScores[orchIdx].data[regionIdx].data = orchScores[orchIdx].data[regionIdx].data.slice(1);
        }
        orchScores[orchIdx].data[regionIdx].data.push({ latency: thisPing, timestamp: now });
        currentDataList = orchScores[orchIdx].data[regionIdx].data;
        break;
      }
      if (!regionFound) {
        currentDataList = [{ latency: thisPing, timestamp: now }];
        orchScores[orchIdx].data.push({ tag, data: currentDataList });
      }
      break;
    }
    if (!orchFound) {
      currentDataList = [{ tag, data: [{ latency: thisPing, timestamp: now }] }];
      orchScores.push({ id, data: currentDataList });
    }
    await storage.setItem('orchScores', orchScores);
    // Calc new scores
    let prevtime = null;
    let uptime = 0;
    let downtime = 0;
    let pingsum = 0;
    let pingpoints = 0;
    for (const thisData of currentDataList) {
      // Count ping* vars
      if (thisData.latency) {
        pingsum += thisData.latency;
        pingpoints += 1;
        promLatestLatency.set({ region: tag, orchestrator: id }, thisData.latency);
        promLatency.observe({ region: tag }, thisData.latency);
      }
      // Only count *time vars if we have timestamps
      if (prevtime && thisData.timestamp) {
        if (thisData.latency) {
          uptime += thisData.timestamp - prevtime;
        } else {
          downtime += thisData.timestamp - prevtime;
        }
      }
      prevtime = thisData.timestamp;
    }
    if (pingpoints) {
      promAverageLatency.set({ region: tag, orchestrator: id }, pingsum / pingpoints);
    }
    if (uptime || downtime) {
      const score = uptime / (uptime + downtime)
      promAUptimeScore.set({ region: tag, orchestrator: id }, score);
    }
    res.send(true);
  } catch (err) {
    console.log(err);
    res.status(400).send(err);
  }
});


/*

Public endpoints

*/


masterRouter.get("/prometheus", async (req, res) => {
  try {
    res.set('Content-Type', register.contentType);
    const metrics = await register.metrics();
    res.end(metrics);
  } catch (err) {
    res.status(400).send(err);
  }
});

masterRouter.get("/json", async (req, res) => {
  try {
    res.set('Content-Type', 'application/json');
    res.end(orchScores);
  } catch (err) {
    res.status(400).send(err);
  }
});


/*

Recover from storage

*/


let orchScores;

const recoverStorage = async function () {
  await storage.init({
    stringify: JSON.stringify,
    parse: JSON.parse,
    encoding: 'utf8',
    logging: false,
    ttl: false,
    forgiveParseErrors: false
  });
  orchScores = await storage.getItem('orchScores');
  if (!orchScores) { orchScores = []; }
  // Init prometheus from storage
  for (const thisOrch of orchScores) {
    console.log("recovering scores for " + thisOrch.id);
    for (const thisRegion of thisOrch.data) {
      let prevtime = null;
      let uptime = 0;
      let downtime = 0;
      let pingsum = 0;
      let pingpoints = 0;
      for (const thisData of thisRegion.data) {
        // Count ping* vars
        if (thisData.latency) {
          pingsum += thisData.latency;
          pingpoints += 1;
          promLatestLatency.set({ region: thisRegion.tag, orchestrator: thisOrch.id }, thisData.latency);
          promLatency.observe({ region: thisRegion.tag }, thisData.latency);
        }
        // Only count *time vars if we have timestamps
        if (prevtime && thisData.timestamp) {
          if (thisData.latency) {
            uptime += thisData.timestamp - prevtime;
          } else {
            downtime += thisData.timestamp - prevtime;
          }
        }
        prevtime = thisData.timestamp;
      }
      if (pingpoints) {
        promAverageLatency.set({ region: thisRegion.tag, orchestrator: thisOrch.id }, pingsum / pingpoints);
      }
      if (uptime || downtime) {
        const score = uptime / (uptime + downtime)
        promAUptimeScore.set({ region: thisRegion.tag, orchestrator: thisOrch.id }, score);
      }
    }
  }
  isSynced = true;
}
recoverStorage();





export default masterRouter;