// src/index.js
import "dotenv/config";                 // local only; harmless in prod
import express from "express";
import cors from "cors";
import functions from "@google-cloud/functions-framework";

import smsRoutes from "./routes/sms.js";
import healthRoutes from "./routes/health.js";
import appointmentsRouter from "./routes/appointments.js";
import dealsRouter from "./routes/deals.js";
import contactsRouter from "./routes/contacts.js";

const app = express();

// CORS (tighten in prod: origin: 'https://your-site')
app.use(cors());
app.use(express.json());

// Routes

app.use("/sms", smsRoutes);
app.use("/deals", dealsRouter);
app.use("/contacts", contactsRouter);
app.use("/health", healthRoutes);

// Export ONE function that handles all routes
functions.http("api", app);
