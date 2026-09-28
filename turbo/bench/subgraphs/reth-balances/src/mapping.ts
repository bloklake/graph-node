import { Address, BigInt } from "@graphprotocol/graph-ts";
import { Approval, Transfer } from "../generated/RETH/ERC20";
import { Account, Allowance, ApprovalEvent, TransferEvent } from "../generated/schema";

function account(address: Address): Account {
  let row = Account.load(address);
  if (row == null) {
    row = new Account(address);
    row.balance = BigInt.zero();
  }
  return row;
}

export function handleTransfer(event: Transfer): void {
  let value = event.params.value;
  // Sequential, so a self-transfer nets to zero.
  let from = account(event.params.from);
  from.balance = from.balance.minus(value);
  from.save();
  let to = account(event.params.to);
  to.balance = to.balance.plus(value);
  to.save();

  let row = new TransferEvent(event.transaction.hash.concatI32(event.logIndex.toI32()));
  row.from = event.params.from;
  row.to = event.params.to;
  row.amount = value;
  row.timestamp = event.block.timestamp.toI32();
  row.save();
}

export function handleApproval(event: Approval): void {
  let owner = event.params.owner;
  let spender = event.params.spender;
  let allowance = new Allowance(owner.concat(spender));
  allowance.owner = owner;
  allowance.spender = spender;
  allowance.amount = event.params.value;
  allowance.save();

  let row = new ApprovalEvent(event.transaction.hash.concatI32(event.logIndex.toI32()));
  row.owner = owner;
  row.spender = spender;
  row.amount = event.params.value;
  row.timestamp = event.block.timestamp.toI32();
  row.save();
}
