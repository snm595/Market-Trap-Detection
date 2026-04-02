const Record = require("../models/Record");
const asyncHandler = require("../middlewares/asyncHandler");

const getSummary = asyncHandler(async (req, res) => {
  const filter = { isDeleted: false };

  const totals = await Record.aggregate([
    { $match: filter },
    {
      $group: {
        _id: null,
        totalIncome: {
          $sum: {
            $cond: [{ $eq: ["$type", "income"] }, "$amount", 0]
          }
        },
        totalExpenses: {
          $sum: {
            $cond: [{ $eq: ["$type", "expense"] }, "$amount", 0]
          }
        }
      }
    }
  ]);

  const categoryWiseTotals = await Record.aggregate([
    { $match: filter },
    {
      $group: {
        _id: {
          category: "$category",
          type: "$type"
        },
        total: { $sum: "$amount" }
      }
    },
    { $sort: { total: -1 } }
  ]);

  const monthlyTrends = await Record.aggregate([
    { $match: filter },
    {
      $group: {
        _id: {
          year: { $year: "$date" },
          month: { $month: "$date" }
        },
        income: {
          $sum: {
            $cond: [{ $eq: ["$type", "income"] }, "$amount", 0]
          }
        },
        expense: {
          $sum: {
            $cond: [{ $eq: ["$type", "expense"] }, "$amount", 0]
          }
        }
      }
    },
    { $sort: { "_id.year": 1, "_id.month": 1 } }
  ]);

  const recentActivity = await Record.find(filter)
    .sort({ date: -1, createdAt: -1 })
    .limit(5)
    .populate("createdBy", "name email");

  const totalIncome = totals[0]?.totalIncome || 0;
  const totalExpenses = totals[0]?.totalExpenses || 0;

  res.json({
    totalIncome,
    totalExpenses,
    netBalance: totalIncome - totalExpenses,
    categoryWiseTotals,
    monthlyTrends,
    recentActivity
  });
});

module.exports = { getSummary };
