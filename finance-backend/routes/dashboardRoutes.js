const express = require("express");
const protect = require("../middlewares/authMiddleware");
const allowRoles = require("../middlewares/roleMiddleware");
const { getSummary } = require("../controllers/dashboardController");

const router = express.Router();

router.get("/", protect, allowRoles("viewer", "analyst", "admin"), getSummary);

module.exports = router;
