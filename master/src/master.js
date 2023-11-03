import express from "express";
import { masterRouter } from "./routes/index.js";
import { CONF_MASTER_PORT } from "./config.js";
// Env variable which determines which DB to connect to
const { NODE_ENV: mode } = process.env;

(async () => {
  try {
    const app = express();
    app.disable("x-powered-by");
    app.use(express.urlencoded({ extended: true, limit: "1000kb" }));
    app.use(express.json({ limit: "1000kb" }));
    const apiRouter = express.Router();
    app.use("/api", apiRouter);
    apiRouter.use("/master", masterRouter);
    // Error handler
    app.use(function (err, req, res, next) {
      res.locals.message = err.message;
      // Also log it to the console
      console.log(
        `${err.status || 500} - ${err.message} - ${req.originalUrl} - ${
          req.method
        } - ${req.ip}`
      );
      // Render the error page
      res.status(err.status || 500);
      res.render("error");
    });

    app.listen(CONF_MASTER_PORT, "0.0.0.0", function () {
      console.log(`Listening on port ${CONF_MASTER_PORT}`);
    });
  } catch (err) {
    console.log(err);
  }
})();
