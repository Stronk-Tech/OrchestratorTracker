import express from "express";
const dns = require("dns");
var geoip = require("geoip-lite");

const orchTesterRouter = express.Router();
import {
  MASTER_DOMAIN,
  MASTER_PORT,
  MASTER_PATH,
  FRIENDLY_NAME,
  PRESHARED_MASTER_KEY,
  CONF_SLEEPTIME,
  CONT_SIG,
  CONF_ORCHINFO_TIMEOUT,
  CONF_BROADCASTER,
  CONF_DNS_TIMEOUT,
} from "../config";

process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";

/*

INIT
imported modules

*/

import { request, gql } from "graphql-request";
const https = require("https");
const http = require("http");
var grpc = require("@grpc/grpc-js");
var protoLoader = require("@grpc/proto-loader");
var packageDefinition = protoLoader.loadSync("src/proto/livepeer.proto", {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
var livepeerProto = grpc.loadPackageDefinition(packageDefinition).net;
const ssl_creds = grpc.credentials.createSsl(null, null, null, {
  checkServerIdentity: () => undefined,
});

/*

Global helper functions

*/

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function shuffle(array) {
  let currentIndex = array.length,
    randomIndex;
  while (currentIndex != 0) {
    randomIndex = Math.floor(Math.random() * currentIndex);
    currentIndex--;
    [array[currentIndex], array[randomIndex]] = [
      array[randomIndex],
      array[currentIndex],
    ];
  }
  return array;
}

/*

Refreshing active orchestrators
Pulls this data from the Livepeer subgraph (https://api.thegraph.com/subgraphs/name/livepeer/arbitrum-one/graphql)
We might want to switch to reading directly from the blockchain
but this would require constant watching for uri updates which is a pain to implement

*/

var activeOrchestrators = [];
let lastUpdated = 0;
let orchDNS = {};

/// Does a GQL query to the subgraph for orchestrator data
const getOrchestrators = async function () {
  console.log("Getting orchestrator data from the subgraph...");
  try {
    const orchQuery = gql`
      {
        transcoders(where: { active: true }, first: 1000) {
          id
          status
          totalStake
          serviceURI
        }
      }
    `;
    let orchData = await request(
      "https://api.thegraph.com/subgraphs/name/livepeer/arbitrum-one",
      orchQuery
    );
    orchData = orchData.transcoders;
    if (!orchData) {
      console.log("Thegraph is probably acting up...");
      return null;
    }
    return orchData;
  } catch (err) {
    console.log(err);
    console.log("Thegraph is probably acting up...");
    return null;
  }
};

/// Refreshes orchestrator data if the subgraph is available
const refreshOrchCache = async function () {
  const now = new Date().getTime();
  // Update cmc once their data has expired
  if (now - lastUpdated > CONF_ORCHINFO_TIMEOUT) {
    const data = await getOrchestrators();
    if (data) {
      activeOrchestrators = data;
      lastUpdated = now;
    }
  }
};

/*

Doing grpc calls to an orchestrator

*/

let currentPool = [];

const postStatistics = async function (
  id,
  discoveryResults,
  lookupResults,
  responseTime
) {
  console.log("Posting stats for " + id + " (ping " + responseTime + " ms)");

  // TODO look at response and log error?
  var postData = JSON.stringify({
    id,
    discoveryResults,
    responseTime,
    lookupResults,
    tag: FRIENDLY_NAME,
    key: PRESHARED_MASTER_KEY,
  });
  var options = {
    hostname: MASTER_DOMAIN,
    port: MASTER_PORT,
    path: MASTER_PATH,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Content-Length": postData.length,
    },
  };
  var req;
  if (MASTER_DOMAIN == "127.0.0.1" || MASTER_DOMAIN == "localhost") {
    req = http.request(options, (res) => {
      // console.log('statusCode:', res.statusCode);
      // console.log('headers:', res.headers);

      res.on("data", (d) => {
        process.stdout.write(
          "Received response " + d + " from " + MASTER_DOMAIN
        );
      });
    });
  } else {
    req = https.request(options, (res) => {
      // console.log('statusCode:', res.statusCode);
      // console.log('headers:', res.headers);

      res.on("data", (d) => {
        process.stdout.write(
          "Received response " + d + " from " + MASTER_DOMAIN
        );
      });
    });
  }
  req.on("error", (e) => {
    console.error("err", e);
  });
  req.write(postData);
  req.end();
};

function hexToBytes(hex) {
  for (var bytes = [], c = 0; c < hex.length; c += 2)
    bytes.push(parseInt(hex.substr(c, 2), 16));
  return bytes;
}

const discoverOrchestrator = async function (target) {
  if (!target) {
    return;
  }
  var client = new livepeerProto.Orchestrator(target, ssl_creds, {
    GRPC_ARG_DEFAULT_AUTHORITY: Math.random().toString(36).substr(2, 5),
  });
  var receivedResults = false;
  var orchestratorInfo;
  const start = new Date().getTime();
  var elapsed = null;
  await client.GetOrchestrator(
    {
      address: hexToBytes(CONF_BROADCASTER),
      sig: CONT_SIG,
    },
    function (err, res) {
      if (err) {
        console.log("Discovery error: ", err.details);
        orchestratorInfo = err.details;
        elapsed = null;
      } else {
        orchestratorInfo = res;
        elapsed = new Date().getTime() - start;
      }
      receivedResults = true;
    }
  );
  while (!receivedResults && new Date().getTime() - start < 4000) {
    await sleep(20);
  }
  grpc.closeClient(client);
  return { discoveryResults: orchestratorInfo, elapsed };
};

const pingOrchestrator = async function (target) {
  if (!target) {
    return;
  }
  var client = new livepeerProto.Orchestrator(target, ssl_creds);
  var receivedResults = false;
  var pingPong;
  const start = new Date().getTime();
  var elapsed = null;
  await client.Ping({ value: "koekjes" }, function (err, res) {
    if (err) {
      console.log("Ping err: ", err.details);
      pingPong = err.details;
      elapsed = null;
    } else {
      pingPong = res;
      elapsed = new Date().getTime() - start;
    }
    receivedResults = true;
  });
  while (!receivedResults && new Date().getTime() - start < 4000) {
    await sleep(20);
  }
  return { pingResults: pingPong, elapsed };
};

async function getIP(hostname) {
  let obj = await dns.promises.lookup(hostname).catch((error) => {
    console.error(error);
  });
  if (obj) {
    return obj.address;
  } else {
    return null;
  }
}

const testOrchestrator = async function (id, target) {
  if (!id.length || !target.length) {
    return;
  }
  const origTarget = new URL(target);
  target = target.replace(/^https?:\/\//, "");
  console.log("Target is  " + target);
  // Resolve DNS
  const now = new Date().getTime();
  if (!orchDNS[id] || now - orchDNS[id].lastTime > CONF_DNS_TIMEOUT) {
    const resolved = await getIP(origTarget.hostname);
    orchDNS[id] = {
      originalTarget: origTarget.origin,
      resolvedTarget: resolved,
      geoLookup: geoip.lookup(resolved),
      geoFrom: FRIENDLY_NAME,
      lastTime: now,
    };
    console.log("Updated DNS and GeoIP data: ", orchDNS[id]);
  }
  // Test orch
  const { discoveryResults, elapsed } = await discoverOrchestrator(target);
  if (discoveryResults && discoveryResults == "insufficient sender reserve") {
    console.log(
      "Ignoring " + id + " for stats due to insufficient sender reserve"
    );
    return;
  }
  await postStatistics(id, discoveryResults, orchDNS[id], elapsed);
};

const refreshPool = function () {
  currentPool = [];
  for (const thisObj of activeOrchestrators) {
    currentPool.push({ id: thisObj.id, target: thisObj.serviceURI });
  }
  shuffle(currentPool);
};

const pingNextOrch = async function () {
  if (!currentPool.length) {
    refreshPool();
  }
  let currentOrch = currentPool.splice(0, 1)[0];
  if (!currentOrch.id || !currentOrch.target) {
    console.log("Skipping Orchestrator with malformed data: ", currentOrch);
    return;
  }
  await testOrchestrator(currentOrch.id, currentOrch.target);
};

/*

Main Loop
Maybe we shouldn't use nodejs as a client...

*/

let cycle = 0;
let isSyncing = false;
// Does the actual looping over last parsed block -> latest block in chain
const handleSync = async function () {
  try {
    cycle++;
    console.log("Starting new cycle #" + cycle);
    isSyncing = true;
    await refreshOrchCache();
    await pingNextOrch();
    isSyncing = false;
    setTimeout(() => {
      handleSync();
    }, CONF_SLEEPTIME);
    return;
  } catch (err) {
    console.log(err);
    isSyncing = false;
    setTimeout(() => {
      handleSync();
    }, CONF_SLEEPTIME);
  }
};
if (!isSyncing) {
  console.log("Starting main loop");
  handleSync();
}

export default orchTesterRouter;
