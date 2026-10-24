'use strict';
const fs=require('node:fs'),path=require('node:path'),os=require('node:os');
let playwright;
for(const candidate of ['playwright',process.env.CIRCUITBOUND_PLAYWRIGHT,path.join(os.homedir(),'.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright')].filter(Boolean)){
  try{playwright=require(candidate);break;}catch{}
}
if(!playwright)throw new Error('Playwright is required for optional browser checks. Set CIRCUITBOUND_PLAYWRIGHT to its package path.');
const executable=[process.env.CIRCUITBOUND_BROWSER,process.platform==='win32'&&'C:/Program Files/Google/Chrome/Application/chrome.exe',process.platform==='win32'&&'C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe'].find(candidate=>candidate&&fs.existsSync(candidate));
module.exports={playwright,launchOptions:{headless:true,...(executable?{executablePath:executable}:{})}};
