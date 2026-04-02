const jwt = require("jsonwebtoken");
const User = require("../models/User");
const asyncHandler = require("../middlewares/asyncHandler");

const generateToken = (id) =>
  jwt.sign({ id }, process.env.JWT_SECRET, {
    expiresIn: process.env.JWT_EXPIRES_IN || "7d"
  });

const register = asyncHandler(async (req, res) => {
  const { name, email, password, role } = req.body;
  const userCount = await User.countDocuments();

  if (userCount > 0) {
    return res.status(403).json({
      message: "Public registration disabled. Use admin user creation."
    });
  }

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
    role: role || "admin",
    isActive: true
  });

  res.status(201).json({
    _id: user._id,
    name: user.name,
    email: user.email,
    role: user.role,
    isActive: user.isActive,
    token: generateToken(user._id)
  });
});

const login = asyncHandler(async (req, res) => {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({ message: "Email and password are required" });
  }

  const user = await User.findOne({ email });

  if (!user) {
    return res.status(401).json({ message: "Invalid credentials" });
  }

  if (!user.isActive) {
    return res.status(403).json({ message: "User account is inactive" });
  }

  const isMatch = await user.matchPassword(password);
  if (!isMatch) {
    return res.status(401).json({ message: "Invalid credentials" });
  }

  res.json({
    _id: user._id,
    name: user.name,
    email: user.email,
    role: user.role,
    isActive: user.isActive,
    token: generateToken(user._id)
  });
});

module.exports = { register, login };
