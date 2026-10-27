// Run only under an explicitly authorized and rate-limited staging campaign.
// HWP_TEST_APPROVED=yes HWP_TEST_ORIGIN=https://staging.example.test
// HWP_ALLOWED_ORIGINS=https://staging.example.test k6 run ...
import http from 'k6/http';
import { check, sleep } from 'k6';

const target = (__ENV.HWP_TEST_ORIGIN || '').replace(/\/$/, '');
const allowed = (__ENV.HWP_ALLOWED_ORIGINS || '').split(',').map(s => s.trim());
if (__ENV.HWP_TEST_APPROVED !== 'yes' || !/^https:\/\/[a-zA-Z0-9._:-]+$/.test(target)
    || !allowed.includes(target)) {
  throw new Error('Authorized HTTPS staging origin and HWP_TEST_APPROVED=yes are required');
}
const vus = Number(__ENV.HWP_VUS || 10);
const duration = __ENV.HWP_DURATION || '1m';
if (!Number.isInteger(vus) || vus < 1 || vus > 2000
    || !/^[1-9][0-9]?[sm]$/.test(duration)) {
  throw new Error('Workers are capped at 1..2000 VUs and 1..99s/m per run');
}
export const options = {
  vus, duration,
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(95)<1000'] },
  discardResponseBodies: true,
};
export default function () {
  const res = http.get(target + '/', { tags: { journey: 'staging-home' },
                                      redirects: 0, timeout: '10s' });
  check(res, { 'HTTP homepage success': (r) => r.status === 200 });
  sleep(1 + Math.random() * 2);
}
