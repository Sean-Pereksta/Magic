const fs=require('node:fs'),path=require('node:path');
const parts=['world','navigation','audio','settlements','forces','ape-tactics','sim','render','render-details','app'];
const shell=fs.readFileSync(path.join(__dirname,'shell.html'),'utf8');
const scripts=parts.map(name=>'<script>\n'+fs.readFileSync(path.join(__dirname,name+'.js'),'utf8')+'\n</script>').join('\n');
const output=shell.replace('</body>',scripts+'\n</body>');
const target=path.join(__dirname,'../apes-together-strong.html');
if(process.argv.includes('--check')){if(!fs.existsSync(target)||fs.readFileSync(target,'utf8')!==output){console.error('Build is stale. Run node game/apes-together-strong/build.cjs');process.exit(1)}}else{fs.writeFileSync(target,output);console.log('Built '+path.relative(process.cwd(),target)+' ('+Buffer.byteLength(output)+' bytes)')}
