// Shared model-ID validation. Never accept a URL, query string or API credential.
export const DEFAULT_GEMINI_MODEL = 'gemini-2.5-flash-lite';
export function normalizeGeminiModel(value) {
  if (typeof value !== 'string') return null;
  const model=value.trim().replace(/^models\//,'');
  return /^gemini-[a-zA-Z0-9][a-zA-Z0-9._-]{0,72}$/.test(model) ? model : null;
}
export function geminiModelSetting(env) {
  const configured=env.GEMINI_MODEL !== undefined && env.GEMINI_MODEL !== null && env.GEMINI_MODEL !== '';
  return { model:normalizeGeminiModel(configured ? env.GEMINI_MODEL : DEFAULT_GEMINI_MODEL), modelSource:configured ? 'configured' : 'default' };
}
