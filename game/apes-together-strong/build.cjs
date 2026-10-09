const fs=require('node:fs'),path=require('node:path');
const parts=['visual-assets','world','navigation','audio','settlements','forces','ape-tactics','siege','sim','arsenal','champions','kingdom','prisons','weather','nearest-orders','progression','equipment','palisade-traversal','settlement-combat','horde-commands','render','render-details','siege-render','arsenal-render','champions-render','prisons-render','weather-render','settlement-render','progression-render','equipment-render','palisade-render','graphics','environment-art','character-art','equipment-art','held-item-art','structure-art','vehicle-art','interface-art','siege-ui','champions-ui','kingdom-ui','equipment-ui','mobile-commands','app'];
const shell=fs.readFileSync(path.join(__dirname,'shell.html'),'utf8');
const assetRoot=path.join(__dirname,'assets','visual'),manifest={},sources={};
for(const family of ['characters','environment','equipment','held-items','settlements','vehicles']){
 const data=JSON.parse(fs.readFileSync(path.join(assetRoot,family,'manifest.json'),'utf8'));
 manifest[family]=data;
 for(const atlas of data.atlases){
  if(sources[atlas.id])throw new Error('Duplicate atlas: '+atlas.id);
  const file=path.resolve(assetRoot,atlas.file);
  if(!file.startsWith(assetRoot+path.sep))throw new Error('Invalid asset path: '+atlas.file);
  const ext=path.extname(file).slice(1).toLowerCase();
  if(!['png','webp'].includes(ext))throw new Error('Atlas must be PNG or WebP: '+atlas.file);
  sources[atlas.id]='data:image/'+ext+';base64,'+fs.readFileSync(file).toString('base64');
 }
}
const bundle='<script>window.ATS_VISUAL_BUNDLE='+JSON.stringify({manifest,sources}).replace(/</g,'\\u003c')+';</script>\n';
const scripts=bundle+parts.map(name=>'<script>\n'+fs.readFileSync(path.join(__dirname,name+'.js'),'utf8')+'\n</script>').join('\n');
const tracks=['underpowered-king','ceremonial-tom','primal-roar'];
const music='<audio id="campaignMusic" loop preload="none"></audio>'+tracks.map(id=>'<script type="text/plain" id="music-source-'+id+'">data:audio/mpeg;base64,'+fs.readFileSync(path.join(__dirname,'assets',id+'.mp3')).toString('base64')+'</script>').join('\n');
const output=shell.replace('<!-- CAMPAIGN_MUSIC -->',music).replace('</body>',scripts+'\n</body>');
const target=path.join(__dirname,'../apes-together-strong.html');
if(process.argv.includes('--check')){if(!fs.existsSync(target)||fs.readFileSync(target,'utf8')!==output){console.error('Build is stale. Run node game/apes-together-strong/build.cjs');process.exit(1)}}else{fs.writeFileSync(target,output);console.log('Built '+path.relative(process.cwd(),target)+' ('+Buffer.byteLength(output)+' bytes)')}
