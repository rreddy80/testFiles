#!/usr/bin/env ts-node

import axios from 'axios';
import { Command } from 'commander';
import * as fs from 'fs';
import path from 'path';
import Table from 'cli-table3';
import { createObjectCsvWriter } from 'csv-writer';
import { fetchToken } from './token-provider';
import { sleep, average } from './utils';

const program = new Command();

program
  .requiredOption('-e, --endpoints <file>', 'Path to endpoints JSON file')
  .option('-c, --concurrency <number>', 'Concurrent users', '10')
  .option('-n, --requests <number>', 'Total requests', '50')
  .option('-d, --delay <ms>', 'Delay per request (ms)', '0')
  .option('--out <file>', 'CSV output file', 'results.csv');

program.parse(process.argv);
const opts = program.opts();

type EndpointConfig = {
  name: string;
  method: string;
  url: string;
  bodyBuilder?: string;
};

type Result = {
  id: number;
  name: string;
  method: string;
  status: number | string;
  latency: number;
};

const results: Result[] = [];

async function loadEndpoints(file: string): Promise<EndpointConfig[]> {
  return JSON.parse(fs.readFileSync(path.resolve(file), 'utf8'));
}

async function sendRequest(id: number, config: EndpointConfig, token: string): Promise<Result> {
  const start = Date.now();
  try {
    const headers = { Authorization: `Bearer ${token}` };
    let body = undefined;

    if (config.method.toUpperCase() === 'POST' && config.bodyBuilder) {
      const builder = await import(`./bodies/${config.bodyBuilder}.js`);
      body = builder.default();
    }

    const response = await axios({
      method: config.method,
      url: config.url,
      headers,
      data: body
    });

    const latency = Date.now() - start;
    return { id, name: config.name, method: config.method, status: response.status, latency };
  } catch (err: any) {
    const latency = Date.now() - start;
    const status = err.response?.status || err.code || 'ERROR';
    return { id, name: config.name, method: config.method, status, latency };
  }
}

async function runWorker(workerId: number, perWorker: number, endpoints: EndpointConfig[], token: string, delay: number) {
  for (let i = 0; i < perWorker; i++) {
    const requestId = workerId * perWorker + i;
    const config = endpoints[requestId % endpoints.length];
    const result = await sendRequest(requestId, config, token);
    results.push(result);
    console.log(`#${result.id} [${result.method}] ${result.latency}ms ${result.status} - ${result.name}`);
    if (delay > 0) await sleep(delay);
  }
}

async function runTest() {
  const endpoints = await loadEndpoints(opts.endpoints);
  const concurrency = parseInt(opts.concurrency);
  const totalRequests = parseInt(opts.requests);
  const delay = parseInt(opts.delay);
  const perWorker = Math.ceil(totalRequests / concurrency);

  console.log('🔐 Fetching bearer token...');
  const token = await fetchToken();

  const startTime = Date.now();

  const workers = Array.from({ length: concurrency }).map((_, i) =>
    runWorker(i, perWorker, endpoints, token, delay)
  );

  await Promise.all(workers);

  const totalTime = Date.now() - startTime;

  // CLI Table Summary
  const table = new Table({
    head: ['Endpoint', 'Method', 'Count', 'Avg Latency (ms)', 'Success', 'Failed']
  });

  const grouped = new Map<string, Result[]>();

  for (const res of results) {
    const key = `${res.name}_${res.method}`;
    if (!grouped.has(key)) grouped.set(key, []);
    grouped.get(key)!.push(res);
  }

  for (const [key, group] of grouped) {
    const [name, method] = key.split('_');
    const latencies = group.map(r => r.latency);
    const successes = group.filter(r => typeof r.status === 'number' && r.status < 400).length;
    const fails = group.length - successes;
    table.push([name, method, group.length, average(latencies).toFixed(2), successes, fails]);
  }

  console.log('\n📊 Summary Table:');
  console.log(table.toString());
  console.log(`⏱ Total time: ${totalTime}ms`);
  await saveCSV(results, opts.out);
  console.log(`📁 Results saved to: ${opts.out}`);
}

async function saveCSV(data: Result[], outFile: string) {
  const csvWriter = createObjectCsvWriter({
    path: path.resolve(outFile),
    header: [
      { id: 'id', title: 'ID' },
      { id: 'name', title: 'Name' },
      { id: 'method', title: 'Method' },
      { id: 'status', title: 'Status' },
      { id: 'latency', title: 'Latency (ms)' }
    ]
  });

  await csvWriter.writeRecords(data);
}

runTest();
