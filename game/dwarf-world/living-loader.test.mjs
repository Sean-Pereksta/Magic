import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const source=fs.readFileSync(new URL('./fortress-command-expansion.js',import.meta.url),'utf8');
const core=fs.readFileSync(new URL('../DwarfWorld-core.html',import.meta.url),'utf8');
function loader(fail=false){const context={window:{},AbortController,setTimeout,clearTimeout,fetch:async url=>({ok:!fail,status:fail?404:200,text:async()=>fs.readFileSync(new URL(url.replace('./dwarf-world/','./'),import.meta.url),'utf8')})};vm.runInNewContext(source,context);return context.window.DwarfWorldFortressExpansion}
test('full expansion injects with LF and CRLF and every script parses together',async()=>{
  for(const input of [core.replace(/\r\n/g,'\n'),core.replace(/\r?\n/g,'\r\n')]){
    const result=await loader().prepareCore(input);assert.match(result,/__DW_LIVING_MOUNTAIN__/);const scripts=[...result.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)].map(m=>m[1]).filter(Boolean);assert.ok(scripts.length);for(const script of scripts)assert.doesNotThrow(()=>new Function(script));assert.equal(await loader().prepareCore(result),result);
  }
});
test('missing runtime rejects launch instead of silently dropping game systems',async()=>{await assert.rejects(loader(true).prepareCore(core),/Could not load/)});
