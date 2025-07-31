import { db } from "ponder:api";
import { account, transferEvent } from "ponder:schema";
import { Hono } from "hono";
import { eq, desc, count, gte, and } from "ponder";

const app = new Hono();

app.get("/balance/:address", async (c) => {
  const address = c.req.param("address") as `0x${string}`;

  const balance = await db
    .select()
    .from(account)
    .where(eq(account.address, address));

  // Convert BigInt values to strings for JSON serialization
  const serializedBalance = balance.map((acc) => ({
    ...acc,
    balance: acc.balance.toString(),
  }));

  return c.json(serializedBalance);
});

app.get("/transfers/to/:address", async (c) => {
  const address = c.req.param("address") as `0x${string}`;
  const page = parseInt(c.req.query("page") || "1");
  const limit = 50;
  const offset = (page - 1) * limit;
  const minAmount = c.req.query("amount");

  // Build where condition
  const conditions = [eq(transferEvent.to, address)];
  if (minAmount) {
    conditions.push(gte(transferEvent.amount, BigInt(minAmount)));
  }
  const whereCondition =
    conditions.length > 1 ? and(...conditions) : conditions[0];

  const transfers = await db
    .select()
    .from(transferEvent)
    .where(whereCondition)
    .orderBy(desc(transferEvent.timestamp))
    .limit(limit)
    .offset(offset);

  const countResult = await db
    .select({ total: count() })
    .from(transferEvent)
    .where(whereCondition);

  const total = countResult[0]?.total ?? 0;

  // Convert BigInt values to strings for JSON serialization
  const serializedTransfers = transfers.map((transfer) => ({
    ...transfer,
    amount: transfer.amount.toString(),
    blockNumber: transfer.blockNumber.toString(),
  }));

  return c.json({
    transfers: serializedTransfers,
    pagination: {
      page,
      limit,
      total,
      totalPages: Math.ceil(total / limit),
    },
  });
});

export default app;
