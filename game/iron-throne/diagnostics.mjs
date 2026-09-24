import { normalizeGeminiModel } from './gemini-model.mjs';
// Only these codes and states cross the diagnostics boundary. Never copy raw
// provider errors, request bodies, credentials, or exception messages into a report.
export const CHECK_NAMES = ['GEMINI_API_KEY', 'TURNSTILE_SECRET', 'BUDGET'];
const STATES = ['unknown', 'missing', 'present', 'verified', 'rejected', 'failed'];
export const CHECK_LABELS = { unknown: 'Unknown — no readable result', missing: 'Missing from the running Worker', present: 'Present — not yet validated', verified: 'Verified during this request', rejected: 'Rejected by the service', failed: 'Present, but its operation failed' };
const CODES = {
  CONFIG_MISSING: ['Worker configuration', 'Required Worker configuration is missing.', 'Open Cloudflare → iron-throne-diplomacy → Settings → Variables and Secrets for the two secrets, and Bindings for BUDGET. Deploy your changes. Build-only variables are unavailable at runtime.'],
  CLIENT_CONFIG: ['Game configuration', 'The game has no usable Gemini connection settings.', 'Check config.json for an HTTPS diplomacyEndpoint and a public turnstileSiteKey.'],
  CONFIG_LOAD_FAILED: ['Game configuration', 'The game could not load config.json.', 'Check that config.json was published with the game, then refresh the page.'],
  TURNSTILE_LOAD_FAILED: ['Browser verification', 'The verification script could not load.', 'Check your connection and whether a browser extension blocks challenges.cloudflare.com, then reopen the council.'],
  TURNSTILE_WIDGET_FAILED: ['Browser verification', 'The Turnstile widget reported a verification error.', 'Check the widget site key and allowed hostnames in Cloudflare Turnstile, then reopen the council.'],
  SESSION_NOT_READY: ['Session setup', 'Verification has not established a diplomacy session.', 'Wait for verification to finish. If it fails, this report will update with the session error.'],
  SESSION_EXPIRED: ['Session setup', 'The Worker rejected an expired or invalid session.', 'Reopen the council to complete fresh verification.'],
  SESSION_RATE_LIMIT: ['Session setup', 'Too many verification attempts were made recently.', 'Wait one minute before reopening the council.'],
  RATE_LIMIT_UNKNOWN: ['Request allowance', 'The Worker rate-limited this request without identifying the allowance.', 'Wait for the retry time. Deploy the updated Worker to distinguish the game allowance from Google’s quota.'],
  SESSION_RESPONSE_INVALID: ['Session setup', 'The Worker returned an unusable session response.', 'Deploy matching game and Worker code, and check the device clock.'],
  SESSION_ISSUE_FAILED: ['Session setup', 'The Worker could not sign a diplomacy session.', 'Inspect the Worker logs and its session signing configuration, then redeploy.'],
  SESSION_TIMEOUT: ['Session setup', 'The session request timed out.', 'Check connectivity and Worker availability, then reopen the council.'],
  REQUEST_TIMEOUT: ['Connection', 'The browser timed out waiting for the conversation response.', 'Check connectivity and Worker logs. This timeout alone cannot establish whether Google received the request.'],
  NETWORK_UNREADABLE: ['Connection', 'The browser could not read a Worker response.', 'Network interruption, browser blocking, or CORS may be responsible. Check the browser Network panel and ALLOWED_ORIGINS. The three server settings cannot be verified from this failure.'],
  WORKER_ACCESS_DENIED: ['Connection', 'Access to the Worker was denied.', 'Check Cloudflare access/security rules and the allowed site origin. A status alone cannot identify which rule rejected the request.'],
  WORKER_ROUTE_MISSING: ['Connection', 'The requested Worker route was not found.', 'Deploy the current Worker alongside the game and check diplomacyEndpoint.'],
  WORKER_UNAVAILABLE: ['Connection', 'The Worker returned an error without structured diagnostics.', 'Deploy the updated Worker to identify missing settings. If it is current, check its logs and the Network response.'],
  ORIGIN_DENIED: ['Connection', 'The Worker does not allow this site origin.', 'Include the game’s exact origin in ALLOWED_ORIGINS, including https://www.catnmice.com when applicable.'],
  IDENTITY_MISSING: ['Session setup', 'Cloudflare did not supply a client identity.', 'Use the deployed Cloudflare Worker endpoint and inspect any intervening proxy.'],
  REQUEST_INVALID: ['Request validation', 'The Worker could not accept the request.', 'Deploy matching game and Worker versions. Check the request status in the Network panel.'],
  TURNSTILE_SECRET_INVALID: ['Server verification', 'Cloudflare rejected the Turnstile secret.', 'Replace TURNSTILE_SECRET with the Secret Key belonging to the configured Turnstile widget and deploy.'],
  TURNSTILE_TOKEN_EXPIRED: ['Server verification', 'The verification token expired or was already used.', 'Reopen the council to obtain a fresh verification token.'],
  TURNSTILE_REJECTED: ['Server verification', 'Cloudflare did not accept the verification token.', 'Check the Turnstile widget configuration and the safe error codes below, then obtain a fresh token.'],
  TURNSTILE_HOSTNAME: ['Server verification', 'The verified hostname does not match the game’s origin.', 'Check the Turnstile widget hostname configuration and the address used to open the game.'],
  TURNSTILE_ACTION: ['Server verification', 'The verification action does not match this game.', 'Deploy the current game files; the widget action must be iron-throne.'],
  TURNSTILE_UNAVAILABLE: ['Server verification', 'The Worker could not complete Cloudflare’s verification request.', 'Check Worker logs and Turnstile availability, then retry verification.'],
  BUDGET_FAILED: ['BUDGET binding', 'The Durable Object could not complete its operation.', 'Confirm BUDGET is a Durable Object binding to DiplomacyBudget, deploy the Wrangler configuration, and inspect Worker/Durable Object logs.'],
  DAILY_LIMIT: ['Game request allowance', 'The Worker’s daily Gemini allowance is exhausted.', 'Wait for the next UTC day or review DAILY_LIMIT against your provider allowance.'],
  CLIENT_RATE_LIMIT: ['Game request allowance', 'This client reached the Worker’s per-minute allowance.', 'Wait before sending another message.'],
  GLOBAL_RATE_LIMIT: ['Game request allowance', 'The shared Worker reached its per-minute allowance.', 'Wait before sending another message.'],
  PROVIDER_COOLDOWN: ['Gemini request', 'Gemini is cooling down after a provider rate limit.', 'Wait for the retry time before sending another message.'],
  GEMINI_KEY_INVALID: ['Gemini request', 'Google rejected the Gemini API key.', 'Check GEMINI_API_KEY in the running Worker and replace it with an active Google AI Studio key if needed.'],
  GEMINI_PERMISSION: ['Gemini request', 'Google refused this project or key access.', 'Check the API key restrictions, enabled API, project access, and account eligibility in Google AI Studio.'],
  GEMINI_BILLING: ['Gemini billing', 'Google reported that payment, billing, or prepaid credits are required.', 'Check billing for the project attached to this API key in Google AI Studio, including any required prepay setup and available balance.'],
  GEMINI_QUOTA: ['Gemini request', 'Google reported an exhausted quota or rate limit.', 'Review the project’s model quotas in Google AI Studio and wait for the relevant reset.'],
  GEMINI_MODEL: ['Gemini request', 'Google could not find or provide the configured model.', 'In Cloudflare → iron-throne-diplomacy → Settings → Variables and Secrets, restore GEMINI_MODEL to a model available for this API key with generateContent support, then deploy. Use worker/check-models.mjs to list available models; a code redeploy alone cannot restore an overwritten value.'],
  GEMINI_MODEL_CONFIG: ['Worker configuration', 'GEMINI_MODEL is not a valid Gemini model ID.', 'Set GEMINI_MODEL to a model ID such as gemini-2.5-flash-lite or models/gemini-2.5-flash-lite, not a URL or API key. Verify availability for your project, then deploy.'],
  GEMINI_REQUEST: ['Gemini request', 'Google rejected the request format or parameters.', 'Check the Worker’s model and response-schema configuration.'],
  GEMINI_TIMEOUT: ['Gemini request', 'The Gemini request timed out.', 'Try again after the cooldown. Check provider availability if it continues.'],
  GEMINI_UNAVAILABLE: ['Gemini request', 'The Worker could not obtain a successful Gemini response.', 'Check provider availability and Worker logs; the upstream status is included when available.'],
  GEMINI_RESPONSE_INVALID: ['Gemini reply', 'Gemini returned an incomplete or invalid structured reply.', 'Retry after the cooldown. If repeated, check the Worker response schema and output limit.'],
  REQUEST_CANCELLED: ['Gemini request', 'The conversation request was cancelled.', 'Send a new message when the campaign is ready.']
};
const TURNSTILE_CODES = ['missing-input-secret', 'invalid-input-secret', 'missing-input-response', 'invalid-input-response', 'bad-request', 'timeout-or-duplicate', 'internal-error'];
export function workerChecks(env) {
  return Object.fromEntries(CHECK_NAMES.map(name => [name, env[name] ? 'present' : 'missing']));
}
export function makeDiagnostic(code, details = {}) {
  return {
    version: 1, code: typeof code === 'string' && Object.hasOwn(CODES, code) ? code : 'WORKER_UNAVAILABLE',
    checks: Object.fromEntries(CHECK_NAMES.map(name => [name, STATES.includes(details.checks?.[name]) ? details.checks[name] : 'unknown'])),
    ...(Number.isInteger(details.providerStatus) && details.providerStatus >= 100 && details.providerStatus <= 599 ? { providerStatus: details.providerStatus } : {}),
    ...(normalizeGeminiModel(details.model) ? { model: normalizeGeminiModel(details.model) } : {}),
    ...(['configured','default'].includes(details.modelSource) ? { modelSource: details.modelSource } : {}),
    turnstileCodes: [...new Set((Array.isArray(details.turnstileCodes) ? details.turnstileCodes : []).filter(c => TURNSTILE_CODES.includes(c)))].slice(0, 7)
  };
}
export function readDiagnostic(value) {
  return value?.version === 1 && typeof value.code === 'string' && Object.hasOwn(CODES, value.code) ? makeDiagnostic(value.code, value) : null;
}
export function diagnosticDetails(value) {
  const d = readDiagnostic(value) || makeDiagnostic('WORKER_UNAVAILABLE');
  const [stage, reason, action] = CODES[d.code];
  const missing = CHECK_NAMES.filter(name => d.checks[name] === 'missing');
  return { stage, reason: d.code === 'CONFIG_MISSING' && missing.length ? `Missing from the running Worker: ${missing.join(', ')}.` : reason, action };
}
export function diagnosticReport(record, endpoint = '', origin = '', clientSettings = {}) {
  if (!record) return '';
  const d = readDiagnostic(record) || makeDiagnostic('WORKER_UNAVAILABLE'), info = diagnosticDetails(d);
  // Only fixed paths and sanitized origins are included, never URL credentials or queries.
  const safeOrigin = value => { try { const u = new URL(value); return ['https:', 'http:'].includes(u.protocol) && !u.username && !u.password ? u.origin : 'Unknown'; } catch { return 'Not configured'; } };
  const path = ['/session', '/diplomacy', '/config.json', '/verification'].includes(record.path) ? record.path : '';
  return [
    'Iron Throne — Gemini diagnostics v1',
    `Time: ${Number.isSafeInteger(record.at) && record.at > 0 && record.at < 8640000000000000 ? new Date(record.at).toISOString() : 'Unknown'}`,
    `Game origin: ${safeOrigin(origin)}`, `Worker: ${safeOrigin(endpoint)}`, `Failed step: ${info.stage}`, `Request: ${path || 'Not sent'}`,
    `HTTP status: ${Number.isInteger(record.httpStatus) && record.httpStatus >= 100 && record.httpStatus <= 599 ? record.httpStatus : 'Unavailable'}`,
    ...(d.providerStatus ? [`Google HTTP status: ${d.providerStatus}`] : []), `Error code: ${d.code}`, `Reason: ${info.reason}`, '',
    ...(d.model ? [`Gemini model: ${d.model}`] : []),
    ...(d.modelSource ? [`Model setting: ${d.modelSource === 'configured' ? 'Cloudflare GEMINI_MODEL' : 'Worker default (GEMINI_MODEL not set)'}`] : []),
    ...CHECK_NAMES.map(name => `${name}: ${CHECK_LABELS[d.checks[name]]}`),
    'Present means configured, not proof that a key is valid or funded.',
    `Game endpoint configured: ${clientSettings.endpoint ? 'Yes' : 'No'}`, `Public Turnstile site key configured: ${clientSettings.siteKey ? 'Yes' : 'No'}`,
    ...(d.turnstileCodes.length ? [`Turnstile error codes: ${d.turnstileCodes.join(', ')}`] : []),
    ...(Number.isSafeInteger(record.retryAt) && record.retryAt > 0 && record.retryAt < 8640000000000000 ? [`Retry after: ${new Date(record.retryAt).toISOString()}`] : []), '',
    `Next step: ${info.action}`, '', 'No keys, tokens, IP addresses, messages, or campaign data are included. This report describes the captured failure; opening it makes no AI request.'
  ].join('\n');
}
