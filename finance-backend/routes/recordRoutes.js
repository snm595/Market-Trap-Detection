const express = require("express");
const protect = require("../middlewares/authMiddleware");
const allowRoles = require("../middlewares/roleMiddleware");
const {
  createRecord,
  getRecords,
  getRecordById,
  updateRecord,
  deleteRecord
} = require("../controllers/recordController");

const router = express.Router();

router.use(protect);

router.get("/", allowRoles("admin", "analyst"), getRecords);
router.get("/:id", allowRoles("admin", "analyst"), getRecordById);
router.post("/", allowRoles("admin"), createRecord);
router.patch("/:id", allowRoles("admin"), updateRecord);
router.delete("/:id", allowRoles("admin"), deleteRecord);

module.exports = router;
