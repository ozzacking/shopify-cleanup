// Shopify Bulk Operations 업로드 스크립트 (SparkCJ 동일 로직)
const https = require('https');
const http = require('http');
const fs = require('fs');
const readline = require('readline');
const { URL } = require('url');

const SHOP = process.env.SHOP;
const TOKEN = process.env.TOKEN;
const JSONL_FILE = process.env.JSONL_FILE || 'products_pika.jsonl';
const API_VERSION = '2026-01';
const BATCH_SIZE = 2500;

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// GraphQL 요청
async function gql(query, variables = {}) {
  const payload = JSON.stringify({ query, variables });
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: SHOP,
      path: `/admin/api/${API_VERSION}/graphql.json`,
      method: 'POST',
      headers: {
        'X-Shopify-Access-Token': TOKEN,
        'Content-Type': 'application/json',
        'Content-Length': Buffer.byteLength(payload)
      }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => {
        try { resolve(JSON.parse(data)); }
        catch(e) { reject(new Error('JSON parse error: ' + data.slice(0, 200))); }
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// Location ID 가져오기 (없으면 null 반환 - 드롭쉬핑은 재고 불필요)
async function getLocationId() {
  try {
    const result = await gql(`query {
      locations(first: 10) {
        nodes { id name isPrimary }
      }
    }`);
    const locations = result.data?.locations?.nodes || [];
    if (locations.length === 0) {
      console.log('[Location] 접근 권한 없음 - 재고 추적 없이 업로드');
      return null;
    }
    const primary = locations.find(l => l.isPrimary) || locations[0];
    console.log('[Location] 사용:', primary.name, primary.id);
    return primary.id;
  } catch(e) {
    console.log('[Location] 조회 실패, 재고 없이 진행:', e.message);
    return null;
  }
}

// 기존 handle 로드 (중복 제거용)
async function loadExistingHandles() {
  console.log('[중복체크] 기존 상품 handle 로딩...');
  const handles = new Set();
  let cursor = null;
  let total = 0;

  while (true) {
    const result = await gql(`query getHandles($cursor: String) {
      products(first: 250, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes { handle }
      }
    }`, { cursor });

    const products = result.data?.products;
    if (!products) break;

    for (const p of products.nodes) {
      if (p.handle) handles.add(p.handle.toLowerCase());
    }
    total += products.nodes.length;
    if (total % 1000 === 0) console.log(`[중복체크] ${total}개 로드됨...`);

    if (!products.pageInfo.hasNextPage) break;
    cursor = products.pageInfo.endCursor;
    await sleep(200);
  }

  console.log(`[중복체크] 완료: ${handles.size}개 기존 상품`);
  return handles;
}

// stagedUploadsCreate
async function stagedUploadsCreate() {
  const result = await gql(`mutation stagedUploadsCreate($filename: String!) {
    stagedUploadsCreate(input: {
      resource: BULK_MUTATION_VARIABLES,
      filename: $filename,
      mimeType: "text/jsonl",
      httpMethod: POST
    }) {
      stagedTargets {
        url
        resourceUrl
        parameters { name value }
      }
    }
  }`, { filename: 'productsJSONL' });

  if (result.errors) throw new Error(JSON.stringify(result.errors));
  const targets = result.data?.stagedUploadsCreate?.stagedTargets;
  if (!targets || targets.length === 0) throw new Error('stagedTargets 없음');
  return targets[0];
}

// S3 업로드
async function uploadToS3(uploadUrl, parameters, jsonlContent) {
  const boundary = '----FormBoundary' + Math.random().toString(36).slice(2);
  const jsonlBuffer = Buffer.from(jsonlContent, 'utf8');

  let formParts = Buffer.alloc(0);
  for (const param of parameters) {
    const part = Buffer.from(
      `--${boundary}\r\nContent-Disposition: form-data; name="${param.name}"\r\n\r\n${param.value}\r\n`
    );
    formParts = Buffer.concat([formParts, part]);
  }

  const fileHeader = Buffer.from(
    `--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="products.jsonl"\r\nContent-Type: text/jsonl\r\n\r\n`
  );
  const fileFooter = Buffer.from(`\r\n--${boundary}--\r\n`);
  const body = Buffer.concat([formParts, fileHeader, jsonlBuffer, fileFooter]);

  const parsedUrl = new URL(uploadUrl);
  const isHttps = parsedUrl.protocol === 'https:';
  const lib = isHttps ? https : http;

  return new Promise((resolve, reject) => {
    const req = lib.request({
      hostname: parsedUrl.hostname,
      path: parsedUrl.pathname + parsedUrl.search,
      method: 'POST',
      headers: {
        'Content-Type': `multipart/form-data; boundary=${boundary}`,
        'Content-Length': body.length
      }
    }, (res) => {
      let data = '';
      res.on('data', c => data += c);
      res.on('end', () => resolve({ status: res.statusCode, body: data }));
    });
    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

// Bulk Operation 실행
async function runBulkOperation(stagedUploadPath) {
  const mutationDoc = `mutation call($input: ProductSetInput!) {
    productSet(input: $input) {
      product { id handle title }
      userErrors { field message code }
    }
  }`;

  for (let attempt = 1; attempt <= 3; attempt++) {
    const result = await gql(`mutation bulkOperationRunMutation($mutation: String!, $stagedUploadPath: String!) {
      bulkOperationRunMutation(mutation: $mutation, stagedUploadPath: $stagedUploadPath) {
        bulkOperation { id url status }
        userErrors { message field }
      }
    }`, { mutation: mutationDoc, stagedUploadPath });

    if (result.errors) {
      const msg = JSON.stringify(result.errors);
      if (msg.includes('THROTTLED') && attempt < 3) {
        console.log('[Throttle] 2분 대기...');
        await sleep(120000);
        continue;
      }
      throw new Error('bulkOperationRunMutation 에러: ' + msg);
    }

    const userErrors = result.data?.bulkOperationRunMutation?.userErrors || [];
    if (userErrors.length > 0) {
      const msg = userErrors.map(e => e.message).join(', ');
      if (msg.includes('already in progress') && attempt < 3) {
        console.log('[BulkOp] 이전 작업 대기 중...');
        await pollUntilDone();
        continue;
      }
      throw new Error('bulkOperation userErrors: ' + msg);
    }

    const op = result.data?.bulkOperationRunMutation?.bulkOperation;
    if (!op) throw new Error('bulkOperation 응답 없음');
    return op;
  }
  throw new Error('bulkOperation 3회 실패');
}

// Bulk Operation 폴링 (ID로 직접 조회)
async function pollBulkOperation(operationId) {
  console.log(`[Poll] 대기 중... id=${operationId}`);
  let attempts = 0;
  let nullCount = 0;

  while (true) {
    await sleep(attempts < 5 ? 10000 : 30000);
    attempts++;

    // ID로 직접 조회
    const result = await gql(`query getOp($id: ID!) {
      node(id: $id) {
        ... on BulkOperation {
          id status errorCode url rootObjectCount
        }
      }
    }`, { id: operationId });

    const op = result.data?.node;

    // node 조회 실패 시 currentBulkOperation 폴백
    if (!op || !op.status) {
      const fallback = await gql(`query { currentBulkOperation { id status errorCode url rootObjectCount } }`);
      const cur = fallback.data?.currentBulkOperation;

      if (!cur) {
        nullCount++;
        if (nullCount >= 3) {
          console.log('[Poll] Bulk Operation 완료됨 (결과 없음)');
          return { success: true, url: null };
        }
        console.log(`[Poll] 응답 없음 (${nullCount}/3), 재시도...`);
        continue;
      }

      if (cur.id === operationId || cur.status === 'COMPLETED') {
        console.log(`[Poll] ${cur.status}`);
        if (cur.status === 'COMPLETED') return { success: true, url: cur.url };
        if (cur.status === 'FAILED') return { success: false, error: cur.errorCode };
      }
      nullCount = 0;
      continue;
    }

    nullCount = 0;
    console.log(`[Poll] ${op.status} (${op.rootObjectCount || 0}개 처리됨)`);

    if (op.status === 'COMPLETED') return { success: true, url: op.url };
    if (op.status === 'FAILED') return { success: false, error: op.errorCode };
    if (op.status === 'CANCELED') return { success: false, error: 'CANCELED' };
  }
}

// 결과 파싱
async function parseResults(resultUrl) {
  if (!resultUrl) return { success: 0, failed: 0, dailyLimit: false };

  return new Promise((resolve) => {
    const parsedUrl = new URL(resultUrl);
    const lib = parsedUrl.protocol === 'https:' ? https : http;

    lib.get(resultUrl, (res) => {
      let success = 0, failed = 0, dailyLimit = false;
      const rl = readline.createInterface({ input: res });

      rl.on('line', (line) => {
        if (!line.trim()) return;
        try {
          const obj = JSON.parse(line);
          const errors = obj.errors || [];
          const dlError = errors.find(e =>
            e?.extensions?.code === 'VARIANT_THROTTLE_EXCEEDED' ||
            e?.message?.includes('Daily variant creation limit')
          );
          if (dlError) { dailyLimit = true; failed++; return; }

          const ps = obj.data?.productSet || obj.productSet;
          if (ps) {
            if (ps.userErrors && ps.userErrors.length > 0) failed++;
            else if (ps.product?.id) success++;
          }
        } catch {}
      });

      rl.on('close', () => resolve({ success, failed, dailyLimit }));
    }).on('error', () => resolve({ success: 0, failed: 0, dailyLimit: false }));
  });
}

// 현재 실행 중인 bulk operation 확인
async function pollUntilDone() {
  while (true) {
    const result = await gql(`query { currentBulkOperation { id status } }`);
    const op = result.data?.currentBulkOperation;
    if (!op || op.status === 'COMPLETED' || op.status === 'FAILED' || op.status === 'CANCELED') break;
    console.log('[Wait] 기존 bulk operation 진행 중:', op.status);
    await sleep(15000);
  }
}

async function main() {
  console.log(`\n🚀 Pikatechno 업로드 시작`);
  console.log(`스토어: ${SHOP}`);

  // 1. Location ID
  const locationId = await getLocationId();

  // 2. 기존 handle 로드
  const existingHandles = await loadExistingHandles();

  // 3. JSONL 파일 로드 + location ID 교체 + 중복 제거
  console.log(`\n[데이터] ${JSONL_FILE} 로딩...`);
  const rawLines = fs.readFileSync(JSONL_FILE, 'utf8').split('\n').filter(l => l.trim());
  console.log(`[데이터] ${rawLines.length}개 로드됨`);

  const lines = [];
  let skippedDup = 0;
  for (const line of rawLines) {
    try {
      const obj = JSON.parse(line);
      const handle = obj.input?.handle?.toLowerCase();
      if (handle && existingHandles.has(handle)) { skippedDup++; continue; }

      // metafield namespace "cj" → "cjdrop" (최소 3자 필요)
      if (obj.input?.metafields) {
        for (const mf of obj.input.metafields) {
          if (mf.namespace === 'cj') mf.namespace = 'cjdrop';
        }
      }

      if (locationId) {
        const replaced = JSON.stringify(obj).replace(/__LOCATION_ID__/g, locationId);
        lines.push(replaced);
      } else {
        // Location 없으면 inventoryQuantities 제거
        if (obj.input?.variants) {
          for (const v of obj.input.variants) {
            delete v.inventoryQuantities;
            v.inventoryItem = { tracked: false };
          }
        }
        lines.push(JSON.stringify(obj));
      }
    } catch {}
  }

  console.log(`[데이터] 업로드 대상: ${lines.length}개 (중복 스킵: ${skippedDup}개)`);
  if (lines.length === 0) { console.log('✅ 업로드할 상품 없음 (모두 중복)'); return; }

  // 4. 배치 처리
  const totalBatches = Math.ceil(lines.length / BATCH_SIZE);
  let totalSuccess = 0, totalFailed = 0, dailyLimitReached = false;

  for (let i = 0; i < lines.length; i += BATCH_SIZE) {
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const batch = lines.slice(i, i + BATCH_SIZE);
    console.log(`\n[Batch ${batchNum}/${totalBatches}] ${batch.length}개 처리 중...`);

    if (dailyLimitReached) {
      const now = new Date();
      const midnight = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1, 0, 3, 0));
      const waitMs = Math.max(midnight.getTime() - now.getTime(), 60000);
      console.log(`[일일 제한] UTC 자정까지 ${Math.round(waitMs/60000)}분 대기...`);
      await sleep(waitMs);
      dailyLimitReached = false;
      console.log('[일일 제한] 리셋 완료, 업로드 재개');
    }

    // 이전 bulk operation 완료 확인
    await pollUntilDone();

    // Staged upload 생성
    let stagedTarget;
    try {
      stagedTarget = await stagedUploadsCreate();
    } catch(e) {
      console.error(`[Batch ${batchNum}] stagedUpload 실패:`, e.message);
      totalFailed += batch.length;
      continue;
    }

    // S3 업로드
    const jsonlContent = batch.join('\n');
    const fileKey = stagedTarget.parameters.find(p => p.name === 'key')?.value;

    let s3Result;
    for (let attempt = 1; attempt <= 3; attempt++) {
      try {
        s3Result = await uploadToS3(stagedTarget.url, stagedTarget.parameters, jsonlContent);
        if (s3Result.status >= 200 && s3Result.status < 300) break;
        if (attempt < 3) { console.log(`[S3] 재시도 ${attempt}/3...`); await sleep(60000); }
      } catch(e) {
        if (attempt < 3) { console.log(`[S3] 에러, 재시도:`, e.message); await sleep(10000); }
        else throw e;
      }
    }

    if (!s3Result || s3Result.status >= 300) {
      console.error(`[Batch ${batchNum}] S3 업로드 실패: ${s3Result?.status}`);
      totalFailed += batch.length;
      continue;
    }

    // Bulk Operation 실행
    let bulkOp;
    try {
      bulkOp = await runBulkOperation(fileKey);
      console.log(`[Batch ${batchNum}] Bulk Operation 시작: ${bulkOp.id}`);
    } catch(e) {
      console.error(`[Batch ${batchNum}] bulkOperation 실패:`, e.message);
      totalFailed += batch.length;
      continue;
    }

    // 완료 대기
    const pollResult = await pollBulkOperation(bulkOp.id);

    // 결과 파싱
    const stats = await parseResults(pollResult.url);
    totalSuccess += stats.success;
    totalFailed += stats.failed;
    if (stats.dailyLimit) dailyLimitReached = true;

    console.log(`[Batch ${batchNum}] 완료 - 성공: ${stats.success}, 실패: ${stats.failed}${stats.dailyLimit ? ' ⚠️ 일일제한' : ''}`);
    console.log(`[전체] 누적 성공: ${totalSuccess}개`);
  }

  console.log('\n========================================');
  console.log('업로드 완료!');
  console.log(`성공: ${totalSuccess}개`);
  console.log(`실패: ${totalFailed}개`);
  console.log('========================================');
}

main().catch(e => { console.error('치명적 에러:', e); process.exit(1); });
