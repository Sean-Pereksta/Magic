const fs=require('node:fs'),path=require('node:path');
const parts=['world','navigation','audio','settlements','forces','ape-tactics','siege','sim','arsenal','champions','kingdom','prisons','weather','nearest-orders','render','render-details','siege-render','arsenal-render','champions-render','prisons-render','weather-render','siege-ui','champions-ui','kingdom-ui','app'];
const shell=fs.readFileSync(path.join(__dirname,'shell.html'),'utf8');
const scripts=parts.map(name=>'<script>\n'+fs.readFileSync(path.join(__dirname,name+'.js'),'utf8')+'\n</script>').join('\n');
const music=fs.readFileSync(path.join(__dirname,'assets/primal-roar.mp3')).toString('base64');
const output=shell.replace('<!-- CAMPAIGN_MUSIC -->','<audio id="campaignMusic" loop preload="none" src="data:audio/mpeg;base64,'+music+'"></audio>').replace('</body>',scripts+'\n</body>');
const target=path.join(__dirname,'../apes-together-strong.html');
if(process.argv.includes('--check')){if(!fs.existsSync(target)||fs.readFileSync(target,'utf8')!==output){console.error('Build is stale. Run node game/apes-together-strong/build.cjs');process.exit(1)}}else{fs.writeFileSync(target,output);console.log('Built '+path.relative(process.cwd(),target)+' ('+Buffer.byteLength(output)+' bytes)')}
