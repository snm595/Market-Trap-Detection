const Record = require("../models/Record");
const asyncHandler = require("../middlewares/asyncHandler");

const createRecord = asyncHandler(async (req, res) => {
  const { amount, type, category, date, notes } = req.body;

  if (amount === undefined || !type || !category) {
    return res.status(400).json({
      message: "Amount, type, and category are required"
    });
  }

  if (!["income", "expense"].includes(type)) {
    return res.status(400).json({ message: "Type must be income or expense" });
  }

  const record = await Record.create({
    amount,
    type,
    category,
    date: date || Date.now(),
    notes: notes || "",
    createdBy: req.user._id
  });

  res.status(201).json(record);
});

const getRecords = asyncHandler(async (req, res) => {
  const {
    type,
    category,
    startDate,
    endDate,
    search,
    page = 1,
    limit = 10
  } = req.query;

  const query = { isDeleted: false };

  if (type) {
    query.type = type;
  }
  if (category) {
    query.category = new RegExp(category, "i");
  }

  if (startDate || endDate) {
    query.date = {};
    if (startDate) {
      query.date.$gte = new Date(startDate);
    }
    if (endDate) {
      query.date.$lte = new Date(endDate);
    }
  }

  if (search) {
    query.$or = [
      { notes: new RegExp(search, "i") },
      { category: new RegExp(search, "i") }
    ];
  }

  const skip = (Number(page) - 1) * Number(limit);
  const safeLimit = Math.max(Number(limit), 1);

  const records = await Record.find(query)
    .populate("createdBy", "name email role")
    .sort({ date: -1, createdAt: -1 })
    .skip(skip)
    .limit(safeLimit);

  const total = await Record.countDocuments(query);

  res.json({
    page: Number(page),
    limit: safeLimit,
    total,
    totalPages: Math.ceil(total / safeLimit),
    records
  });
});

const getRecordById = asyncHandler(async (req, res) => {
  const record = await Record.findOne({
    _id: req.params.id,
    isDeleted: false
  }).populate("createdBy", "name email role");

  if (!record) {
    return res.status(404).json({ message: "Record not found" });
  }

  res.json(record);
});

const updateRecord = asyncHandler(async (req, res) => {
  const record = await Record.findOne({
    _id: req.params.id,
    isDeleted: false
  });

  if (!record) {
    return res.status(404).json({ message: "Record not found" });
  }

  const { amount, type, category, date, notes } = req.body;

  if (amount !== undefined) {
    record.amount = amount;
  }
  if (type !== undefined) {
    if (!["income", "expense"].includes(type)) {
      return res.status(400).json({ message: "Type must be income or expense" });
    }
    record.type = type;
  }
  if (category !== undefined) {
    record.category = category;
  }
  if (date !== undefined) {
    record.date = date;
  }
  if (notes !== undefined) {
    record.notes = notes;
  }

  await record.save();

  res.json(record);
});

const deleteRecord = asyncHandler(async (req, res) => {
  const record = await Record.findOne({
    _id: req.params.id,
    isDeleted: false
  });

  if (!record) {
    return res.status(404).json({ message: "Record not found" });
  }

  record.isDeleted = true;
  await record.save();

  res.json({ message: "Record deleted successfully" });
});

module.exports = {
  createRecord,
  getRecords,
  getRecordById,
  updateRecord,
  deleteRecord
};
