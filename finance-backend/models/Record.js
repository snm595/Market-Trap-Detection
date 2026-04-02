const mongoose = require("mongoose");

const recordSchema = new mongoose.Schema(
  {
    amount: {
      type: Number,
      required: [true, "Amount is required"],
      min: [0, "Amount must be positive"]
    },
    type: {
      type: String,
      enum: ["income", "expense"],
      required: [true, "Type is required"]
    },
    category: {
      type: String,
      required: [true, "Category is required"],
      trim: true
    },
    date: {
      type: Date,
      default: Date.now
    },
    notes: {
      type: String,
      trim: true,
      default: ""
    },
    createdBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "User",
      required: true
    },
    isDeleted: {
      type: Boolean,
      default: false
    }
  },
  { timestamps: true }
);

module.exports = mongoose.model("Record", recordSchema);
