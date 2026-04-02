const express = require("express");
const protect = require("../middlewares/authMiddleware");
const allowRoles = require("../middlewares/roleMiddleware");
const {
  createUser,
  getUsers,
  updateUser,
  deactivateUser
} = require("../controllers/userController");

const router = express.Router();

router.use(protect);
router.use(allowRoles("admin"));

router.post("/", createUser);
router.get("/", getUsers);
router.patch("/:id", updateUser);
router.patch("/:id/status", deactivateUser);

module.exports = router;
