const User = require("../models/User");
const asyncHandler = require("../middlewares/asyncHandler");

const createUser = asyncHandler(async (req, res) => {
  const { name, email, password, role, isActive } = req.body;

  if (!name || !email || !password) {
    return res
      .status(400)
      .json({ message: "Name, email, password are required" });
  }

  const existingUser = await User.findOne({ email });
  if (existingUser) {
    return res.status(409).json({ message: "User already exists" });
  }

  const user = await User.create({
    name,
    email,
    password,
    role: role || "viewer",
    isActive: typeof isActive === "boolean" ? isActive : true
  });

  res.status(201).json({
    _id: user._id,
    name: user.name,
    email: user.email,
    role: user.role,
    isActive: user.isActive
  });
});

const getUsers = asyncHandler(async (req, res) => {
  const users = await User.find().select("-password").sort({ createdAt: -1 });
  res.json(users);
});

const updateUser = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { name, email, role, isActive, password } = req.body;

  const user = await User.findById(id);
  if (!user) {
    return res.status(404).json({ message: "User not found" });
  }

  if (name !== undefined) {
    user.name = name;
  }
  if (email !== undefined) {
    user.email = email;
  }
  if (role !== undefined) {
    user.role = role;
  }
  if (isActive !== undefined) {
    user.isActive = isActive;
  }
  if (password !== undefined) {
    user.password = password;
  }

  await user.save();

  res.json({
    _id: user._id,
    name: user.name,
    email: user.email,
    role: user.role,
    isActive: user.isActive
  });
});

const deactivateUser = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const user = await User.findById(id);
  if (!user) {
    return res.status(404).json({ message: "User not found" });
  }

  user.isActive = false;
  await user.save();

  res.json({ message: "User deactivated successfully" });
});

module.exports = {
  createUser,
  getUsers,
  updateUser,
  deactivateUser
};
