import { ponder } from "ponder:registry";
import { account, transferEvent } from "ponder:schema";

ponder.on("cbBTC:Transfer", async ({ event, context }) => {
  // track if an insertion happened, if so, update the accounts.
  let transferEventResult = await context.db.insert(transferEvent).values({
    id: event.id,
    amount: event.args.amount,
    timestamp: Number(event.block.timestamp),
    from: event.args.from,
    to: event.args.to,
    transactionHash: event.transaction.hash,
    blockNumber: event.block.number,
    blockHash: event.block.hash,
  }).onConflictDoNothing();

  if (transferEventResult) {
    await context.db
      .insert(account)
      .values({ address: event.args.from, balance: 0n })
      .onConflictDoUpdate((row) => ({
        balance: row.balance - event.args.amount,
      }));

    await context.db
      .insert(account)
      .values({
        address: event.args.to,
        balance: event.args.amount,
      })
      .onConflictDoUpdate((row) => ({
        balance: row.balance + event.args.amount,
      }));
  }
});
