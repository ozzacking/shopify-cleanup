const https = require('https');
const [,, SHOP, TOKEN, STORE_NAME] = process.argv;
const API_VERSION = '2026-01';

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

async function getProducts() {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: SHOP,
      path: `/admin/api/${API_VERSION}/products.json?limit=250&fields=id`,
      method: 'GET',
      headers: { 'X-Shopify-Access-Token': TOKEN }
    };
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
        catch(e) { reject(e); }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function deleteProduct(id) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: SHOP,
      path: `/admin/api/${API_VERSION}/products/${id}.json`,
      method: 'DELETE',
      headers: { 'X-Shopify-Access-Token': TOKEN }
    };
    const req = https.request(options, (res) => {
      res.on('data', () => {});
      res.on('end', () => resolve(res.statusCode));
    });
    req.on('error', reject);
    req.end();
  });
}

async function main() {
  let totalDeleted = 0, totalFailed = 0, round = 0;
  console.log(`[${STORE_NAME}] 삭제 시작: ${SHOP}`);

  while (true) {
    round++;
    let res;
    try {
      res = await getProducts();
    } catch(e) {
      console.log(`[${STORE_NAME}] 목록 조회 에러: ${e.message}, 5초 후 재시도...`);
      await sleep(5000);
      continue;
    }

    if (res.status === 429) {
      console.log(`[${STORE_NAME}] Rate limit, 10초 대기...`);
      await sleep(10000);
      continue;
    }

    const products = res.body.products;
    if (!products || products.length === 0) {
      console.log(`[${STORE_NAME}] 완료! 총 삭제: ${totalDeleted}개, 실패: ${totalFailed}개`);
      break;
    }

    console.log(`[${STORE_NAME}] Round ${round}: ${products.length}개 발견, 삭제 중...`);

    for (const p of products) {
      let status;
      try {
        status = await deleteProduct(p.id);
      } catch(e) {
        console.log(`[${STORE_NAME}] 삭제 에러 id=${p.id}: ${e.message}`);
        totalFailed++;
        await sleep(1000);
        continue;
      }

      if (status === 200) {
        totalDeleted++;
      } else if (status === 429) {
        console.log(`[${STORE_NAME}] Rate limit, 10초 대기...`);
        await sleep(10000);
        totalFailed++;
      } else {
        console.log(`[${STORE_NAME}] 삭제 실패 id=${p.id}, status=${status}`);
        totalFailed++;
      }
      await sleep(300);
    }

    console.log(`[${STORE_NAME}] Round ${round} 완료. 누적 삭제: ${totalDeleted}개`);
  }
}

main().catch(e => {
  console.error(`[${STORE_NAME}] 치명적 에러:`, e);
  process.exit(1);
});
