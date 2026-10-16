"use strict";
const { createRouter } = require("./http.cjs");
const DEFAULT_ORIGINS = ["https://catnmice.com", "https://www.catnmice.com"];
async function mountMountOnline(
  app,
  { db, auth, io, projectId, allowedOrigins, logger = console } = {},
) {
  let service, timer, pushTimer;
  try {
    if (projectId !== "bible-game-246c0") throw new Error("PROJECT_MISMATCH");
    const origins =
      allowedOrigins ||
      (process.env.MOUNTONLINE_ALLOWED_ORIGINS
        ? process.env.MOUNTONLINE_ALLOWED_ORIGINS.split(",")
            .map((s) => s.trim())
            .filter(Boolean)
        : DEFAULT_ORIGINS);
    const { MountService } = await import("./service.mjs");
    service = new MountService({ db, auth });
    await service.acquire();
    app.use(
      "/api/mountonline",
      createRouter({ service, allowedOrigins: origins }),
    );
    let namespace;
    if (io) {
      namespace = io.of("/mountonline");
      namespace.use(async (socket, next) => {
        try {
          const origin = socket.handshake.headers.origin;
          if (origin && !origins.includes(origin))
            throw new Error("ORIGIN_DENIED");
          socket.data.who = await service.identity(socket.handshake.auth.token);
          next();
        } catch {
          next(new Error("Firebase sign-in or origin was not accepted."));
        }
      });
      namespace.on("connection", (socket) => {
        let count = 0,
          windowStart = Date.now();
        const expiry = setTimeout(() => socket.disconnect(true), 50 * 60000);
        expiry.unref?.();
        service
          .join(socket.data.who)
          .then((s) => socket.emit("realm", s))
          .catch((error) => {
            socket.emit("realmError", {
              code: error.code,
              message: error.message,
            });
            socket.disconnect(true);
          });
        socket.on("order", async (input, ack) => {
          try {
            if (Date.now() - windowStart > 60000) {
              windowStart = Date.now();
              count = 0;
            }
            if (++count > 180)
              throw Object.assign(new Error("Please slow down your orders."), {
                code: "RATE_LIMITED",
              });
            const result = await service.request(socket.data.who, input);
            if (typeof ack === "function") ack(result);
          } catch (error) {
            if (typeof ack === "function")
              ack({
                error: {
                  code: error.code,
                  message: error.status
                    ? error.message
                    : "This order could not be saved. Reconnect and retry.",
                },
              });
          }
        });
        socket.on("heartbeat", () => service.heartbeat(socket.data.who.uid));
        socket.on("disconnect", () => clearTimeout(expiry));
      });
      pushTimer = setInterval(() => {
        if (!service.ready || service.saving) return;
        for (const socket of namespace.sockets.values()) {
          const uid = socket.data.who.uid;
          if (service.realm.players.has(uid)) {
            service.heartbeat(uid);
            socket.emit("realm", service.snapshot(uid, socket.data.who.linked));
          }
        }
      }, 200);
      pushTimer.unref?.();
    }
    let busy = false,
      last = Date.now(),
      acc = 0,
      lastRecovery = 0;
    timer = setInterval(async () => {
      if (busy) return;
      busy = true;
      const now = Date.now();
      acc = Math.min(0.5, acc + (now - last) / 1000);
      last = now;
      try {
        if (!service.ready) {
          if (now - lastRecovery > 5000) {
            lastRecovery = now;
            await service.acquire();
          }
          acc = 0;
        } else
          while (acc >= 0.05) {
            await service.advance(0.05);
            acc -= 0.05;
          }
      } catch (error) {
        service.ready = false;
        namespace?.emit("realmError", {
          code: "CHECKPOINT_FAILED",
          message:
            "The realm is reconnecting to its saved checkpoint. Please wait.",
        });
        logger.error(
          "Mountain Blade authority paused.",
          error.code || error.name,
        );
      } finally {
        busy = false;
      }
    }, 50);
    timer.unref?.();
    const close = async () => {
      clearInterval(timer);
      clearInterval(pushTimer);
      namespace?.disconnectSockets(true);
      await service.close();
    };
    return { service, close };
  } catch (error) {
    clearInterval(timer);
    clearInterval(pushTimer);
    logger.error(
      "Mountain Blade Online is unavailable; other games remain available.",
      error.code || error.name,
    );
    return null;
  }
}
module.exports = { mountMountOnline, DEFAULT_ORIGINS };
