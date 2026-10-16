// Optional standalone host. The normal production integration is server.js.
const express = require("express"),
  http = require("node:http"),
  path = require("node:path"),
  fs = require("node:fs"),
  admin = require("firebase-admin"),
  { Server } = require("socket.io"),
  { mountMountOnline } = require("./mount.cjs");
async function main() {
  const serviceAccount = process.env.FIREBASE_SERVICE_ACCOUNT
    ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
    : JSON.parse(
        fs.readFileSync(path.resolve("serviceAccountKey.json"), "utf8"),
      );
  if (serviceAccount.project_id !== "bible-game-246c0")
    throw new Error("Use the existing CatnMice Firebase project.");
  admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
  const app = express(),
    server = http.createServer(app),
    origins = (
      process.env.MOUNTONLINE_ALLOWED_ORIGINS ||
      "https://catnmice.com,https://www.catnmice.com"
    )
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean),
    io = new Server(server, { cors: { origin: origins } }),
    root = path.resolve(__dirname, "../..");
  const host = await mountMountOnline(app, {
    db: admin.firestore(),
    auth: admin.auth(),
    projectId: serviceAccount.project_id,
    io,
    allowedOrigins: origins,
  });
  if (!host) throw new Error("The realm authority could not start.");
  app.use(
    "/game/mountonline",
    express.static(path.join(root, "game/mountonline")),
  );
  app.use(
    "/graphics/mountonline",
    express.static(path.join(root, "graphics/mountonline"), { maxAge: "1h" }),
  );
  app.get("/game/mountonline.html", (_req, res) =>
    res.sendFile(path.join(root, "game/mountonline.html")),
  );
  app.get("/lobby/firebase-config.mjs", (_req, res) =>
    res.sendFile(path.join(root, "lobby/firebase-config.mjs")),
  );
  server.listen(Number(process.env.PORT) || 3000, "0.0.0.0", () =>
    console.log("Mountain Blade realm host is ready."),
  );
  for (const signal of ["SIGINT", "SIGTERM"])
    process.once(signal, async () => {
      await host.close();
      io.close();
      server.close(() => process.exit(0));
    });
}
main().catch((error) => {
  console.error("Mountain Blade startup failed:", error.code || error.name);
  process.exitCode = 1;
});
