import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
const root=fileURLToPath(new URL('./',import.meta.url));
const types={'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.png':'image/png','.jpg':'image/jpeg'};
http.createServer(async(req,res)=>{try{const url=new URL(req.url,'http://localhost');let relative=decodeURIComponent(url.pathname);if(relative.endsWith('/'))relative+='index.html';const file=path.resolve(root,'.'+relative);if(!file.startsWith(root)){res.writeHead(403);res.end();return;}const body=await readFile(file);res.writeHead(200,{'Content-Type':types[path.extname(file)]||'application/octet-stream','Cache-Control':'no-cache'});res.end(body);}catch{res.writeHead(404);res.end('Not found');}}).listen(Number(process.env.PORT)||57291,'127.0.0.1',()=>console.log('Local: http://127.0.0.1:'+(process.env.PORT||57291)));


