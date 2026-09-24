// Run locally with GEMINI_API_KEY in the environment; never paste the key in chat.
// Lists metadata only. It does not call generateContent or change Cloudflare.
import { pathToFileURL } from 'node:url';
import { normalizeGeminiModel, geminiModelSetting } from '../gemini-model.mjs';
export async function listTextModels(apiKey, fetcher=fetch) {
  if (!apiKey) throw new Error('Set GEMINI_API_KEY in your local environment first.');
  const names=new Set(),seen=new Set();let token='';
  for(let page=0;page<20;page++) {
    const url=new URL('https://generativelanguage.googleapis.com/v1beta/models');
    url.searchParams.set('pageSize','100');if(token)url.searchParams.set('pageToken',token);
    let response;
    try {response=await fetcher(url.href,{headers:{'x-goog-api-key':apiKey},signal:AbortSignal.timeout(10000)});}
    catch {throw new Error('Model listing could not reach Google. Check your connection and retry.');}
    if(!response.ok)throw new Error(`Model listing failed (Google HTTP ${response.status}). Check the key, API restrictions and project access in AI Studio.`);
    let body;try{body=await response.json();}catch{throw new Error('Google returned an invalid model list.');}
    if(!Array.isArray(body.models))throw new Error('Google returned an invalid model list.');
    for(const model of body.models){const name=normalizeGeminiModel(model?.name);if(name&&Array.isArray(model?.supportedGenerationMethods)&&model.supportedGenerationMethods.includes('generateContent'))names.add(name);}
    if(!body.nextPageToken)return [...names].sort();
    if(typeof body.nextPageToken!=='string'||body.nextPageToken.length>2048||seen.has(body.nextPageToken))throw new Error('Google returned invalid model pagination.');
    token=body.nextPageToken;seen.add(token);
  }
  throw new Error('Model listing exceeded its page limit.');
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href){
  try {
    const names=await listTextModels(process.env.GEMINI_API_KEY);
    const setting=geminiModelSetting(process.env);
    console.log(`Configured/default model: ${setting.model||'invalid'} (${setting.modelSource})`);
    console.log(`Listed with generateContent support: ${names.includes(setting.model)?'yes':'no'}`);
    console.log('Available Gemini text model IDs:');for(const name of names)console.log(name);
    console.log('Choose a model with the required free-tier availability in your project, then set GEMINI_MODEL in the Cloudflare Worker runtime settings and deploy. Listing metadata does not verify generation, schema support, quota or billing.');
  }catch(error){console.error(error.message);process.exitCode=1;}
}
