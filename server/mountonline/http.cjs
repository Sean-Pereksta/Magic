"use strict";
function createRouter({ service, allowedOrigins }) {
  const express = require("express"),
    router = express.Router(),
    windows = new Map();
  router.use((req, res, next) => {
    const origin = req.get("Origin");
    if (origin && !allowedOrigins.includes(origin))
      return res.status(403).json({
        error: {
          code: "ORIGIN_DENIED",
          message: "This origin is not configured for the realm.",
        },
      });
    if (origin) {
      res.set("Access-Control-Allow-Origin", origin);
      res.set("Vary", "Origin");
    }
    res.set("Cache-Control", "no-store");
    res.set("X-Content-Type-Options", "nosniff");
    if (req.method === "OPTIONS") {
      res.set("Access-Control-Allow-Headers", "Authorization, Content-Type");
      res.set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
      return res.sendStatus(204);
    }
    next();
  });
  router.use(express.json({ limit: "24kb" }));
  router.get("/health", (_req, res) =>
    res
      .status(service.ready ? 200 : 503)
      .json({ service: "mountonline", version: 2, ready: service.ready }),
  );
  router.use(async (req, res, next) => {
    try {
      req.who = await service.identity(
        /^Bearer (\S+)$/.exec(req.get("Authorization") || "")?.[1],
      );
      const now = Date.now();
      let window = windows.get(req.who.uid);
      if (!window || window.until < now) {
        window = { until: now + 60000, count: 0 };
        windows.set(req.who.uid, window);
      }
      if (++window.count > 180)
        return res.status(429).json({
          error: {
            code: "RATE_LIMITED",
            message: "Please wait a moment before retrying.",
          },
        });
      if (windows.size > 5000)
        for (const [id, value] of windows)
          if (value.until < now) windows.delete(id);
      next();
    } catch (error) {
      next(error);
    }
  });
  const route = (fn) => async (req, res, next) => {
    try {
      res.json(await fn(req));
    } catch (error) {
      next(error);
    }
  };
  router.post(
    "/join",
    route((req) => service.join(req.who)),
  );
  router.get(
    "/state",
    route((req) =>
      service.serial(() => {
        service.assertReady();
        service.heartbeat(req.who.uid);
        return service.snapshot(req.who.uid, req.who.linked);
      }),
    ),
  );
  router.post(
    "/action",
    route((req) => service.request(req.who, req.body)),
  );
  router.use((error, _req, res, _next) => {
    const authError = String(error.code || "").startsWith("auth/"),
      status =
        error.status ||
        (authError
          ? 401
          : error.type === "entity.too.large"
            ? 413
            : error instanceof SyntaxError
              ? 400
              : 503);
    res.status(status).json({
      error: {
        code: authError
          ? "UNAUTHENTICATED"
          : error.code || "SERVICE_UNAVAILABLE",
        message: authError
          ? "Your Firebase session has expired. Reconnect."
          : error.status
            ? error.message
            : "The realm could not save this request. Reconnect and retry safely.",
      },
    });
  });
  return router;
}
module.exports = { createRouter };
