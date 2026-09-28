import { Transfer } from "../generated/USDC/ERC20";
import { TransferEvent } from "../generated/schema";

export function handleTransfer(event: Transfer): void {
  let row = new TransferEvent(event.transaction.hash.concatI32(event.logIndex.toI32()));
  row.from = event.params.from;
  row.to = event.params.to;
  row.amount = event.params.value;
  row.timestamp = event.block.timestamp.toI32();
  row.save();
}
