const http = require("node:http"),
  fs = require("node:fs"),
  path = require("node:path");
const root = path.resolve(__dirname, "../../..");
const hooks = `\nglobalThis.__MBO_TEST__={get realm(){return realm},get state(){return state},get battle(){return battle},get mode(){return mode},get renderer(){return renderer},get team(){return team},get pending(){return pending},get network(){return network},get motion(){return remoteMotion},get context(){return selectedContext},feedRemote(snapshot){const first=mode!=="online";if(first){network?.stop();network=null;mode="online";realm=null;state=null;battle=null;dismissed.clear();resultShown.clear();remoteMotion.reset();}remoteState(snapshot);if(first)showGame();},perform,returnMenu,selectGroup,openPanel,practice};\n`;
function serve(req, res) {
  try {
    const url = new URL(req.url, "http://localhost"),
      file = path.resolve(root, "." + decodeURIComponent(url.pathname));
    if (!file.startsWith(root + path.sep) || !fs.statSync(file).isFile())
      throw new Error("Not found");
    let body = fs.readFileSync(file);
    if (url.pathname === "/game/mountonline/app.mjs")
      body = Buffer.from(body.toString() + hooks);
    if (url.pathname === "/game/mountonline/config.mjs")
      body = Buffer.from("export const ART_BASE='/graphics/mountonline/';");
    res.setHeader(
      "Content-Type",
      {
        ".mjs": "text/javascript",
        ".js": "text/javascript",
        ".html": "text/html",
        ".json": "application/json",
        ".css": "text/css",
        ".webp": "image/webp",
        ".png": "image/png",
        ".svg": "image/svg+xml",
      }[path.extname(file)] || "application/octet-stream",
    );
    res.end(body);
  } catch {
    res.statusCode = 404;
    res.end("Not found");
  }
}
module.exports = {
  serve,
  start: () =>
    new Promise((resolve) => {
      const server = http.createServer(serve);
      server.listen(0, "127.0.0.1", () =>
        resolve({
          server,
          origin: "http://127.0.0.1:" + server.address().port,
        }),
      );
    }),
};
